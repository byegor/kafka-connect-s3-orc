package byegor.kafka.connect.orc;

import byegor.kafka.connect.ConnectorConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User:egor
 */
public class TestUtils {

    public static Map<String, String> createConfig() {
        Map<String, String> conf = new HashMap<>();
        conf.put(ConnectorConfig.NAME, "test-connector");
        conf.put(ConnectorConfig.FLUSH_SIZE, "10");
        conf.put(ConnectorConfig.ROTATE_SCHEDULE_INTERVAL_MS, "1000000");

        conf.put(ConnectorConfig.S3_BUCKET, "test");
        conf.put("hadoop.fs.s3a.access.key", "access");
        conf.put("hadoop.fs.s3a.secret.key", "secret");
        conf.put("hadoop.fs.s3a.endpoint", BaseTestWithS3.S3_TEST_URL);
        conf.put("hadoop.fs.file.impl", WindowsTestFileSystem.class.getName());
        conf.put(ConnectorConfig.ROTATE_UNION_INTERVAL_MS, "2000");
        conf.put("aws.accessKeyId", "2000");
        conf.put("aws.secretKey", "2000");
        return conf;
    }

    public static Configuration getHadoopConfiguration(ConnectorConfig connectorConfig) {
        Configuration cnf = new Configuration();
        String prefix = "hadoop.";
        int length = prefix.length() + 1;
        Map<String, Object> config = connectorConfig.getConfig();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String propertyKey = entry.getKey();
            if (propertyKey.startsWith(prefix)) {
                String hadoopKey = propertyKey.substring(length);
                cnf.set(hadoopKey, (String) entry.getValue());
            }
        }
        return cnf;
    }

    public static List<Object[]> getDataFromFile(String filePath) {
        List<Object[]> result = new ArrayList<>();
        Reader reader = null;
        try {
            reader = OrcFile.createReader(new org.apache.hadoop.fs.Path(filePath), OrcFile.readerOptions(new Configuration()));
            try (RecordReader rows = reader.rows()) {
                VectorizedRowBatch batch = reader.getSchema().createRowBatch();
                BytesColumnVector id = (BytesColumnVector) batch.cols[0];
                LongColumnVector count = (LongColumnVector) batch.cols[1];
                LongColumnVector bool = (LongColumnVector) batch.cols[2];
                TimestampColumnVector date = (TimestampColumnVector) batch.cols[3];
                while (rows.nextBatch(batch)) {
                    for (int row = 0; row < batch.size; row++) {

                        result.add(new Object[]{//TODO test for null values
                                id.toString(row),
                                count.vector[row],
                                bool.vector[row],
                                date.getTime(row)
                        });
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Issue on reading orc file", e);
        }
        return result;
    }

    public static List<SinkRecord> generateRecords(int size, int partitions) {
        List<SinkRecord> result = new ArrayList<>();
        Schema schema = createConnectSchema();
        for (int i = 0; i < size; i++) {
            int partNumber = i % partitions;
            SinkRecord record = new SinkRecord(
                    "test-topic",
                    partNumber,
                    null, "",
                    schema, createValue(schema, i),
                    i,
                    System.currentTimeMillis(), TimestampType.CREATE_TIME);
            result.add(record);
        }
        return result;
    }

    public static Object createValue(Schema schema, int cnt) {
        Struct struct = new Struct(schema);
        struct.put("id", "" + cnt);
        struct.put("count", (long) cnt);
        struct.put("bool", cnt % 2 == 0);
        struct.put("date", new java.util.Date());
        return struct;
    }

    public static Schema createConnectSchema() {
        SchemaBuilder builder = new SchemaBuilder(Schema.Type.STRUCT);
        builder.field("id", Schema.STRING_SCHEMA);
        builder.field("count", Schema.INT64_SCHEMA);
        builder.field("bool", Schema.BOOLEAN_SCHEMA);
        builder.field("date", Timestamp.SCHEMA);
        return builder.build();
    }

}
