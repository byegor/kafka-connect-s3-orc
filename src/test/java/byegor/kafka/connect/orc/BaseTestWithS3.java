package byegor.kafka.connect.orc;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.findify.s3mock.S3Mock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.BasicConfigurator;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;


public class BaseTestWithS3 {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static final String S3_TEST_URL = "http://127.0.0.1:9191";

    private static S3Mock s3mock;

    protected AmazonS3 s3Client = newS3Client();

    @BeforeClass
    public static void setUp() throws Exception {
        BasicConfigurator.configure();
        System.setProperty("hadoop.home.dir", new File("target\\test-classes\\hadoop-2.9.2").getAbsolutePath());
        File s3mockDir = temporaryFolder.newFolder("s3-tests");
        s3mock = S3Mock.create(9191, s3mockDir.getCanonicalPath());
        s3mock.start();
        newS3Client().createBucket("test");
    }


    @AfterClass
    public static void tearDown() {
        s3mock.shutdown();
    }

    static AmazonS3 newS3Client() {
        AwsClientBuilder.EndpointConfiguration endpoint = new AwsClientBuilder.EndpointConfiguration(S3_TEST_URL, "eu-west-1");
        AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(endpoint)
                .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                .build();
        return client;
    }

}
