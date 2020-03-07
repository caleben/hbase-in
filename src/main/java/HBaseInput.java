import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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
    private Connection connection;

    public HBaseInput() {
        this.connection = createConnection(createHadoopConf());
    }

    private Configuration createHadoopConf() {
        String path = USER_DIR + File.separator + CONF_DIR;
        LOG.info("path: " + path);
        Configuration conf;
        conf = HBaseConfiguration.create();
        conf.addResource(new Path(path, "hbase-site.xml"));
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

    private void input() throws IOException {
        Admin admin = connection.getAdmin();
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(Constant.FAMILY);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Constant.NAMESPACE, Constant.TABLE));

    }

    private void createTable(String namespace, String table, byte[][] splits) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(namespace, table);
            if (!admin.tableExists(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addFamily(new HColumnDescriptor(Constant.FAMILY));
                admin.createTable(tableDescriptor, splits);
                LOG.info("===== create table completed [{}] =====", tableName.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("get hbase admin error!!!", e);
        }
    }

    private void deleteTable(String namespace, String table) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(namespace, table);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            LOG.info("===== delete table completed [{}] =====", tableName.toString());
        } catch (IOException e) {
            throw new RuntimeException("get HBase admin error!!!", e);
        }
    }

    public static void main(String[] args) {
        HBaseInput baseInput = new HBaseInput();
        byte[][] splits = {"01".getBytes()};
        baseInput.createTable(Constant.NAMESPACE, Constant.TABLE, splits);
    }
}
