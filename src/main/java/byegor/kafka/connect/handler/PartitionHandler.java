package byegor.kafka.connect.handler;

import byegor.kafka.connect.ConnectorConfig;
import byegor.kafka.connect.orc.OrcWriter;
import com.amazonaws.thirdparty.joda.time.DateTime;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.errors.SchemaProjectorException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class PartitionHandler {

    private static Logger log = LoggerFactory.getLogger(PartitionHandler.class);

    private final Map<String, String> commitFiles;
    private final Map<String, OrcWriter> writers;
    private final Map<String, Schema> currentSchemas;
    private final TopicPartition tp;
    private String topicsDir;
    private State state;
    private final Queue<SinkRecord> buffer;
    private final SinkTaskContext context;
    private int recordCount;
    private final int flushSize;
    private final long rotateScheduleIntervalMs;
    private long nextScheduledRotation;
    private long currentOffset;
    private String currentEncodedPartition;
    private Long offsetToCommit;
    private final Map<String, Long> startOffsets;
    private long timeoutMs;
    private long failureTime;
    private final String dirDelim;
    private final Time time;
    private static final Time SYSTEM_TIME = new SystemTime();

    public PartitionHandler(TopicPartition tp,
                            ConnectorConfig connectorConfig,
                            SinkTaskContext context) {
        this(tp, connectorConfig, context, SYSTEM_TIME);
    }

    // Visible for testing
    PartitionHandler(TopicPartition tp,
                     ConnectorConfig connectorConfig,
                     SinkTaskContext context,
                     Time time) {
        this.time = time;
        this.tp = tp;
        this.context = context;
        flushSize = connectorConfig.getInt(ConnectorConfig.FLUSH_SIZE);
        topicsDir = connectorConfig.getString(ConnectorConfig.TOPICS_DIR);

        rotateScheduleIntervalMs =
                connectorConfig.getLong(ConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS);

        timeoutMs = connectorConfig.getLong(ConnectorConfig.RETRY_BACKOFF_MS);

        buffer = new LinkedList<>();
        commitFiles = new HashMap<>();
        writers = new HashMap<>();
        currentSchemas = new HashMap<>();
        startOffsets = new HashMap<>();
        state = State.WRITE_STARTED;
        failureTime = -1L;
        currentOffset = -1L;
        dirDelim = connectorConfig.getString(ConnectorConfig.DIRECTORY_DELIM);

        // Initialize scheduled rotation timer if applicable
        setNextScheduledRotation();
    }

    private enum State {
        WRITE_STARTED,
        WRITE_PARTITION_PAUSED,
        SHOULD_ROTATE,
        FILE_COMMITTED;

        private static final State[] VALS = values();

        public State next() {
            return VALS[(ordinal() + 1) % VALS.length];
        }
    }

    public void write() {
        long now = time.milliseconds();
        if (failureTime > 0 && now - failureTime < timeoutMs) {
            return;
        }

        while (!buffer.isEmpty()) {
            try {
                executeState(now);
            } catch (SchemaProjectorException | IllegalWorkerStateException e) {
                throw new ConnectException(e);
            } catch (RetriableException e) {
                log.error("Exception on topic partition {}: ", tp, e);
                failureTime = time.milliseconds();
                setRetryTimeout(timeoutMs);
                break;
            }
        }
        commitOnTimeIfNoData(now);
    }

    @SuppressWarnings("fallthrough")
    private void executeState(long now) {
        switch (state) {
            case WRITE_STARTED:
                pause();
                nextState();
                // fallthrough
            case WRITE_PARTITION_PAUSED:
                SinkRecord record = buffer.peek();
                Schema valueSchema = record.valueSchema();
                String encodedPartition = encodePartition(record);
                Schema currentValueSchema = currentSchemas.get(encodedPartition);
                if (currentValueSchema == null) {
                    currentSchemas.put(encodedPartition, valueSchema);
                }

                if (!checkRotationOrAppend(
                        record,
                        encodedPartition,
                        now
                )) {
                    break;
                }
                // fallthrough
            case SHOULD_ROTATE:
                commitFiles();
                nextState();
                // fallthrough
            case FILE_COMMITTED:
                setState(State.WRITE_PARTITION_PAUSED);
                break;
            default:
                log.error("{} is not a valid state to write record for topic partition {}.", state, tp);
        }
    }

    private String encodePartition(SinkRecord record) {
        LocalDate localDate = LocalDate.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault());
        return record.topic() + dirDelim + localDate.getYear() + dirDelim + localDate.getMonth();
    }

    /**
     * Check if we should rotate the file (schema change, time-based).
     *
     * @returns true if rotation is being performed, false otherwise
     */
    private boolean checkRotationOrAppend(
            SinkRecord record,
            String encodedPartition,
            long now
    ) {
        if (rotateOnTime(now)) {
            setNextScheduledRotation();
            nextState();
        } else {
            currentEncodedPartition = encodedPartition;
            writeRecord(record);
            buffer.poll();
            if (rotateOnSize()) {
                log.info(
                        "Starting commit and rotation for topic partition {} with start offset {}",
                        tp,
                        startOffsets
                );
                nextState();
                // Fall through and try to rotate immediately
            } else {
                return false;
            }
        }
        return true;
    }

    private void commitOnTimeIfNoData(long now) {
        if (buffer.isEmpty()) {
            // committing files after waiting for rotateIntervalMs time but less than flush.size
            // records available
            if (recordCount > 0 && rotateOnTime(now)) {
                log.info(
                        "Committing files after waiting for rotateIntervalMs time but less than flush.size "
                                + "records available."
                );
                setNextScheduledRotation();

                try {
                    commitFiles();
                } catch (ConnectException e) {
                    log.error("Exception on topic partition {}: ", tp, e);
                    failureTime = time.milliseconds();
                    setRetryTimeout(timeoutMs);
                }
            }

            resume();
            setState(State.WRITE_STARTED);
        }
    }

    public void close() throws ConnectException {
        log.debug("Closing TopicPartitionWriter {}", tp);
        Exception ex = null;
        for (Map.Entry<String, OrcWriter> entry : writers.entrySet()) {
            try {

                //TODO should discard all changes or commit offset and upload data
                entry.getValue().close();
            } catch (Exception e) {
                log.error("Failed to close writer for path " + entry.getKey());
                ex = e;
            }
        }
        writers.clear();
        startOffsets.clear();

        if (ex != null) {
            throw new ConnectException(ex);
        }
    }

    public void buffer(SinkRecord sinkRecord) {
        buffer.add(sinkRecord);
    }

    public Long getOffsetToCommitAndReset() {
        Long latest = offsetToCommit;
        offsetToCommit = null;
        return latest;
    }

    private void nextState() {
        state = state.next();
    }

    private void setState(State state) {
        this.state = state;
    }

    private boolean rotateOnTime(long now) {
        if (recordCount <= 0) {
            return false;
        }

        boolean scheduledRotation = rotateScheduleIntervalMs > 0 && now >= nextScheduledRotation;
        log.trace(
                "Should apply scheduled rotation: (rotateScheduleIntervalMs: '{}', nextScheduledRotation:"
                        + " '{}', now: '{}')? {}",
                rotateScheduleIntervalMs,
                nextScheduledRotation,
                now,
                scheduledRotation
        );
        return scheduledRotation;
    }

    private void setNextScheduledRotation() {
        if (rotateScheduleIntervalMs > 0) {
            long now = time.milliseconds();
            nextScheduledRotation = now + rotateScheduleIntervalMs; //TODO check if its correct
            log.trace(
                    "Update scheduled rotation timer. Next rotation for {} will be at {}",
                    tp,
                    new DateTime(nextScheduledRotation)
            );

        }
    }

    private boolean rotateOnSize() {
        boolean messageSizeRotation = recordCount >= flushSize;
        log.trace(
                "Should apply size-based rotation (count {} >= flush size {})? {}",
                recordCount,
                flushSize,
                messageSizeRotation
        );
        return messageSizeRotation;
    }

    private void pause() {
        log.trace("Pausing writer for topic-partition '{}'", tp);
        context.pause(tp);
    }

    private void resume() {
        log.trace("Resuming writer for topic-partition '{}'", tp);
        context.resume(tp);
    }

    private OrcWriter getWriter(SinkRecord record, String encodedPartition)
            throws ConnectException {
        if (writers.containsKey(encodedPartition)) {
            return writers.get(encodedPartition);
        }
        String commitFilename = getCommitFilename(encodedPartition);
        log.debug(
                "Creating new writer encodedPartition='{}' filename='{}'",
                encodedPartition,
                commitFilename
        );
        OrcWriter writer = new OrcWriter(commitFilename, record.valueSchema());
        writers.put(encodedPartition, writer);
        return writer;
    }

    private String getCommitFilename(String encodedPartition) {
        String commitFile;
        if (commitFiles.containsKey(encodedPartition)) {
            commitFile = commitFiles.get(encodedPartition);
        } else {
            long startOffset = startOffsets.get(encodedPartition);
            commitFile = fileKeyToCommit(encodedPartition, startOffset);
            commitFiles.put(encodedPartition, commitFile);
        }
        return commitFile;
    }

    private String fileKey(String topicsPrefix, String keyPrefix, String name) {
        String suffix = keyPrefix + dirDelim + name;
        return StringUtils.isNotBlank(topicsPrefix)
                ? topicsPrefix + dirDelim + suffix
                : suffix;
    }

    private String fileKeyToCommit(String dirPrefix, long startOffset) {
        String name = tp.topic()
                + "-"
                + tp.partition()
                + "-"
                +  startOffset
                + ".csv";
        return fileKey(topicsDir, dirPrefix, name);
    }

    private void writeRecord(SinkRecord record) {
        currentOffset = record.kafkaOffset();

        if (!startOffsets.containsKey(currentEncodedPartition)) {
            log.trace(
                    "Setting writer's start offset for '{}' to {}",
                    currentEncodedPartition,
                    currentOffset
            );
            startOffsets.put(currentEncodedPartition, currentOffset);
        }

        OrcWriter writer = getWriter(record, currentEncodedPartition);
        writer.write(record);
        ++recordCount;
    }

    private void commitFiles() {
        for (Map.Entry<String, String> entry : commitFiles.entrySet()) {
            commitFile(entry.getKey());
            log.debug("Committed {} for {}", entry.getValue(), tp);
        }
        offsetToCommit = currentOffset + 1;
        commitFiles.clear();
        currentSchemas.clear();
        recordCount = 0;
        log.info("Files committed to S3. Target commit offset for {} is {}", tp, offsetToCommit);
    }

    private void commitFile(String encodedPartition) {
        if (!startOffsets.containsKey(encodedPartition)) {
            log.warn("Tried to commit file with missing starting offset partition: {}. Ignoring.", encodedPartition);
            return;
        }

        if (writers.containsKey(encodedPartition)) {
            // Commits the file and closes the underlying output stream.
            try {
                OrcWriter writer = writers.get(encodedPartition);
                writer.close();
                writers.remove(encodedPartition);
                log.debug("Removed writer for '{}'", encodedPartition);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        startOffsets.remove(encodedPartition);
    }

    private void setRetryTimeout(long timeoutMs) {
        context.timeout(timeoutMs);
    }
}
