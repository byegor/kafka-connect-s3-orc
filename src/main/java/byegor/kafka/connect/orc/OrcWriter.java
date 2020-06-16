package byegor.kafka.connect.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrcWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrcWriter.class);

//    public static final String FILE_PATH = "s3a://<bucket>/<dir>/<file>";
    private final Writer writer;
    private final VectorizedRowBatch batch;
    private final DataParser dataParser;

    public OrcWriter(String fileName, Configuration hadoopCnf, Schema connectSchema) {
        try {
            dataParser = new DataParser();
            TypeDescription orcSchema = SchemaConverter.toOrcSchema(connectSchema); //todo move out
            batch = orcSchema.createRowBatch();
            writer = createWriter(fileName, hadoopCnf, orcSchema);
        } catch (IOException e) {
            throw new ConnectException("Failed to create writer for file " + fileName, e);
        }
    }

    public void write(SinkRecord record) {
        log.trace("Sink record: {}", record);
        try {
            dataParser.writeStructDataToOrc(batch.cols, (Struct) record.value(), batch.size++);
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        } catch (Exception e) {
            throw new ConnectException("Issue on writing record into orc format, record: " + record, e);
        }
    }

    private Writer createWriter(String filePath, Configuration hadoopCnf, TypeDescription schema) throws IOException {
        Path path = new Path(filePath);
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(hadoopCnf).setSchema(schema);
        return OrcFile.createWriter(path, writerOptions);
    }

    /*private OrcFile.WriterOptions getWriterOptions(TypeDescription schema) {
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.
        hadoopConfig.set("fs.s3a.access.key", "s3a://<bucket>/<dir>/<file>");
        hadoopConfig.set("fs.s3a.secret.key", "s3a://<bucket>/<dir>/<file>");
        hadoopConfig.set("fs.s3a.multipart.size", "16M");
        hadoopConfig.set("fs.s3a.multipart.purge.age", "172800");
        return OrcFile.writerOptions(hadoopConfig).setSchema(schema);
    }*/

    @Override
    public void close() throws Exception {
        if (batch.size != 0) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }
}
