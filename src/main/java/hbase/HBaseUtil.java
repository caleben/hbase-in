package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.Constant;

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
    private static HBaseUtil hbaseUtil = new HBaseUtil();

    public HBaseUtil() {
        this.connection = createConnection(createHadoopConf());
    }

    public static void loadData(List<String> stringList) throws IOException {
        byte[][] splits = {"01".getBytes()};

        hbaseUtil.deleteTableIfExists(Constant.NAMESPACE, Constant.TABLE);
        hbaseUtil.createTableIfAbsent(Constant.NAMESPACE, Constant.TABLE, splits);

        hbaseUtil.loadData(Constant.NAMESPACE, Constant.TABLE, stringList);
        hbaseUtil.closeResource();
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

    private void loadData(String namespace, String tableName, List<String> stringList) throws IOException {
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
                putList.add(put);
                puts.add(putList);
                putList = new ArrayList<>();
            }
        }
        //余数不足eachSize的也要加进去
        if (!putList.isEmpty()) {
            puts.add(putList);
        }

        return puts;
    }

    private void createTableIfAbsent(String namespace, String table, byte[][] splits) {
        try {
            Admin admin = connection.getAdmin();
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            TableName tableName = TableName.valueOf(namespace, table);
            if (!admin.tableExists(tableName)) {
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(Constant.FAMILY))
                                .setMaxVersions(1)
                                .setCompressionType(Compression.Algorithm.SNAPPY)
                                .build())
                        .build();
                admin.createTable(tableDescriptor, splits);
                LOG.info("===== create table completed [{}] =====", tableName.toString());
            } else {
                LOG.info("===== table already exists, do nothing! [{}] =====", tableName.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("get hbase admin error!!!", e);
        }
    }

    private void deleteTableIfExists(String namespace, String table) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(namespace, table);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                LOG.info("===== [delete table done] table is [{}] =====", tableName.toString());
            } else {
                LOG.info("===== [delete table false] table not exists [{}] =====", tableName.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("get HBase admin error!!!", e);
        }
    }

    private void closeResource() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
