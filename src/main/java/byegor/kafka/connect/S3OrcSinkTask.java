package byegor.kafka.connect;

import byegor.kafka.connect.handler.PartitionHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class S3OrcSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(S3OrcSinkTask.class);

    private ConnectorConfig s3ConnectorConfig;
    private final Set<TopicPartition> assignment;
    private final Map<TopicPartition, PartitionHandler> topicWriters;

    /**
     * No-arg constructor. Used by Connect framework.
     */
    public S3OrcSinkTask() {
        assignment = new HashSet<>();
        topicWriters = new HashMap<>();
    }


    public void start(Map<String, String> props) {
        try {
            s3ConnectorConfig = new ConnectorConfig(props);
            open(context.assignment());
            log.info("Started S3 connector task with assigned partitions: {}", assignment);
        } catch (Exception e) {
            throw new ConnectException("Failed to start kafka connect", e);
        }
    }

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void open(Collection<TopicPartition> assignedPartitions) {
        assignment.addAll(assignedPartitions);

        for (TopicPartition topicPartition : assignedPartitions) {
            assignedPartitions.add(topicPartition);
            topicWriters.put(topicPartition, new PartitionHandler(topicPartition, s3ConnectorConfig, context));
        }
    }


    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        for (SinkRecord record : records) {
            String topic = record.topic();
            int partition = record.kafkaPartition();
            TopicPartition tp = new TopicPartition(topic, partition);
            topicWriters.get(tp).buffer(record);
        }
        log.debug("Read {} records from Kafka", records.size());

        for (TopicPartition tp : assignment) {
            topicWriters.get(tp).write();
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // No-op. The connector is managing the offsets.
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            PartitionHandler partitionHandler = topicWriters.get(topicPartition);
            Long offsetToCommitAndReset = partitionHandler.getOffsetToCommitAndReset();
            log.trace("Forwarding to framework request to commit offset: {} for {}", offsetToCommitAndReset, topicPartition);
            offsetsToCommit.put(topicPartition, new OffsetAndMetadata(offsetToCommitAndReset));
        }

        return offsetsToCommit;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : assignment) {

            try {
                topicWriters.get(topicPartition).close();
            } catch (ConnectException e) {
                log.error("Error closing writer for " + topicPartition, e);
            }
        }
        topicWriters.clear();
        assignment.clear();
    }

    @Override
    public void stop() {

    }

}
