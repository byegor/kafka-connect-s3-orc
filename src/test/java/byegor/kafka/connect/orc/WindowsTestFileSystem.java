package byegor.kafka.connect.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public class WindowsTestFileSystem extends LocalFileSystem {

    public WindowsTestFileSystem() {
    }

    public WindowsTestFileSystem(Configuration conf) {
       super();
       fs.setConf(conf);
       setConf(conf);
   }

   @Override
   public void setPermission(Path p, FsPermission permission) throws IOException {
       //do nothing
   }

    @Override
    public String getScheme() {
        return "file";
    }
}
