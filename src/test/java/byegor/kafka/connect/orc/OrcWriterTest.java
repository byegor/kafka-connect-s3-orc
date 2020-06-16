package byegor.kafka.connect.orc;

import byegor.kafka.connect.ConnectorConfig;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OrcWriterTest extends BaseTestWithS3 {

    @Before
    public void linuxOnly() {
        String os = System.getProperty("os.name").toLowerCase();
        org.junit.Assume.assumeTrue(!os.contains("win"));
    }

    @Test
    public void testWrite() throws Exception {
        String filePath = "s3a://test/topic/data.orc";
        ConnectorConfig connectorConfig = new ConnectorConfig(TestUtils.createConfig());
        OrcWriter orcWriter = new OrcWriter(filePath, connectorConfig.getHadoopConfig(), TestUtils.createConnectSchema());
        List<SinkRecord> sinkRecords = TestUtils.generateRecords(1, 1);
        orcWriter.write(sinkRecords.get(0));
        orcWriter.close();

        S3Object test = s3Client.getObject("test", "topic/data.orc");
        S3ObjectInputStream objectContent = test.getObjectContent();
        File file = temporaryFolder.newFile();
        IOUtils.copy(objectContent, new FileOutputStream(file));


        List<Object[]> dataFromFile = TestUtils.getDataFromFile(file.getPath());
        Object[] row = dataFromFile.get(0);
        assertEquals("id not correct", "0", row[0]);
        assertEquals("count not correct", 0L, row[1]);
        assertEquals("boolean not correct", 1L, row[2]);
        assertTrue("date not correct", System.currentTimeMillis() - (Long) row[3] < 5_000);
    }
}