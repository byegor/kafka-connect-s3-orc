package byegor.kafka.connect;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConnectorConfig {

    public static final String NAME = "name";

    public static final String S3_BUCKET = "s3.bucket.name";
    public static final String S3_REGION = "s3.region";
    public static final String S3_REGION_DEFAULT = "eu-west-1";
    public static final String S3_ACCESS_KEY = "aws.accessKeyId";
    public static final String S3_ACCESS_SECRET_KEY = "aws.secretKey";

    public static final String TOPICS_DIR = "topics.dir";
    public static final String FLUSH_SIZE = "flush.size";
    public static final String ROTATE_SCHEDULE_INTERVAL_MS = "rotate.schedule.interval.ms";
    public static final String ROTATE_UNION_INTERVAL_MS = "rotate.union.interval.ms";
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    public static final String DIRECTORY_DELIM = "directory.delim";

    public static final String LOCAL_TMP_DIR = "tmp.dir";


    private final Map<String, Object> config;

    public ConnectorConfig(Map<String, String> props) {
        ConfigDef config = getConfig();
        Map<String, Object> parsedConfig = config.parse(props);
        this.config = parsedConfig;
    }

    public <T> T getProperty(String propery, Class<T> clazz) {
        return (T) config.get(propery);
    }

    public <T> Class<T> getClass(String property) {
        return (Class<T>) config.get(property);
    }


    public long getLong(String property) {
        return getProperty(property, Long.class);
    }

    public int getInt(String property) {
        return getProperty(property, Integer.class);
    }

    public String getString(String property) {
        return getProperty(property, String.class);
    }

    public String getName() {
        return (String) config.get(NAME);
    }

    public static ConfigDef getConfig() {
        ConfigDef configDef = new ConfigDef();

        final String group = "S3";
        int orderInGroup = 0;


        configDef.define(
                NAME,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "Connector name",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "S3 Connector name"
        );

        configDef.define(
                S3_BUCKET,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "The S3 Bucket.",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "S3 Bucket"
        );

        configDef.define(
                S3_REGION,
                ConfigDef.Type.STRING,
                S3_REGION_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "The AWS region to be used the connector.",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "AWS region"
        );

        configDef.define(
                S3_ACCESS_KEY,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "IAM access key",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "AWS Access Key"
        );

        configDef.define(
                S3_ACCESS_SECRET_KEY,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                "IAM access secret key",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "AWS Access Secret Key"
        );


        configDef.define(
                TOPICS_DIR,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                "Bucket key prefix",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Root folder path"
        );

        configDef.define(
                FLUSH_SIZE,
                ConfigDef.Type.INT,
                ConfigDef.Importance.HIGH,
                "Number of rows in file before rollover",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Number of rows in file before rollover"
        );

        configDef.define(
                ROTATE_SCHEDULE_INTERVAL_MS,
                ConfigDef.Type.LONG,
                "-1",
                ConfigDef.Importance.MEDIUM,
                "Schedule rollover every X ms, doesn't depend on rollover by size",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Time between rollovers"
        );

        configDef.define(
                RETRY_BACKOFF_MS,
                ConfigDef.Type.LONG,
                "500",
                ConfigDef.Importance.MEDIUM,
                "time to wait in case of some issue",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Rertry action in case of exception in X ms"
        );
        configDef.define(
                DIRECTORY_DELIM,
                ConfigDef.Type.STRING,
                "/",
                ConfigDef.Importance.MEDIUM,
                "Directory delimiter in aws",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Directory delimiter"
        );

        configDef.define(
                LOCAL_TMP_DIR,
                ConfigDef.Type.STRING,
                "/tmp",
                ConfigDef.Importance.MEDIUM,
                "Temporary directory for keeping orc files",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Tmp dir"
        );

        configDef.define(
                ROTATE_UNION_INTERVAL_MS,
                ConfigDef.Type.LONG,
                "" + TimeUnit.DAYS.toMillis(2),
                ConfigDef.Importance.MEDIUM,
                "Interval between union file generation",
                group,
                ++orderInGroup,
                ConfigDef.Width.LONG,
                "Interval between union file generation"
        );
        return configDef;
    }
}

