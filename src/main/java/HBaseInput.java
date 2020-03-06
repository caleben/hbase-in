import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author wenci 2020/3/6 19:43
 */
public class HBaseInput {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseInput.class);
    public static final String CONF_DIR = "conf";
    public static final String USER_DIR = System.getProperty("user.dir");
    private Configuration conf;

    private Configuration createHadoopConf() {
        String path = USER_DIR + File.separator + CONF_DIR;
        LOG.info("path: " + path);
        Configuration conf;
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(path, "hbase-site.xml"));
        conf.addResource(new Path(path, "core-site.xml"));
        conf.addResource(new Path(path, "hdfs-site.xml"));
        conf.set("hbase.client.scanner.timeout.period", "90000");
        return conf;
    }

    private Connection createConnection(Configuration configuration) {
        try {
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException("get HBase connection error!!!", e);
        }
    }
}
