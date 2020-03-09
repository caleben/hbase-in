import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wenci 2020/3/6 19:43
 */
public class HBaseUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseUtil.class);
    public static final String CONF_DIR = "conf";
    public static final String USER_DIR = System.getProperty("user.dir");
    private Connection connection;

    public HBaseUtil() {
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

    public void put(String namespace, String tableName, List<String> stringList) throws IOException {
        Instant start = Instant.now();
        for (List<Put> putList : genPut(stringList, 1000)) {
            Table table = connection.getTable(TableName.valueOf(namespace, tableName));
            table.put(putList);
        }
        LOG.info("===== put data succeed! size:{}, cost:{}ms", stringList.size(), (ChronoUnit.MILLIS.between(start, Instant.now())));
    }

    public static void main(String[] args) {
        HBaseUtil hBaseUtil = new HBaseUtil();
        hBaseUtil.genPut(GenData.genData4CourseInfo(Constant.AMOUNT, 2), 5);
    }

    private List<List<Put>> genPut(List<String> stringList, int eachSize) {
        List<List<Put>> puts = new ArrayList<>();
        List<Put> putList = new ArrayList<>();
        for (String line : stringList) {
            String[] n = line.split("\t");
            String rowKey = n[0];
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i = 0; i < Constant.COLUMNS.length; i++) {
                put.addColumn(Bytes.toBytes(Constant.FAMILY), Bytes.toBytes(Constant.COLUMNS[i]), Bytes.toBytes(n[i + 1]));
            }
            if (putList.size() < eachSize) {
                putList.add(put);
            }
            if (putList.size() >= eachSize) {
                puts.add(putList);
                putList.add(put);
                putList = new ArrayList<>();
            }
        }
        //余数不足eachSize的也要加进去
        if (!putList.isEmpty()) {
            puts.add(putList);
        }

        return puts;
    }

    public void createTable(String namespace, String table, byte[][] splits) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(namespace, table);
            if (!admin.tableExists(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                tableDescriptor.addFamily(new HColumnDescriptor(Constant.FAMILY));
                admin.createTable(tableDescriptor, splits);
                LOG.info("===== create table completed [{}] =====", tableName.toString());
            } else {
                LOG.info("===== table already exists, do nothing! [{}] =====", tableName.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("get hbase admin error!!!", e);
        }
    }

    public void deleteTable(String namespace, String table) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(namespace, table);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                LOG.info("===== delete table completed [{}] =====", tableName.toString());
            } else {
                LOG.info("===== table not exists [{}] =====", tableName.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("get HBase admin error!!!", e);
        }
    }

    public void closeResource() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
