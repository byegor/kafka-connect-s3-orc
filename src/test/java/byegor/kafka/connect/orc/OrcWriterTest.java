package byegor.kafka.connect.orc;

import byegor.kafka.connect.ConnectorConfig;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class OrcWriterTest extends BaseTestWithS3 {


    @Test
    public void testWrite() throws Exception {
        System.out.println("HOME: " + System.getProperty("hadoop.home.dir"));

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
        assertArrayEquals(new Object[]{1}, dataFromFile.get(0));
    }
}