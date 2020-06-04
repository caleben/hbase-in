import es.EsUtil;
import hbase.GenData;
import hbase.HBaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.Constant;
import tool.FileUtil;
import tool.Records2File;
import tool.Tool;

import java.io.IOException;
import java.util.List;

/**
 * @author wenci 2020/3/7 15:50
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static List<String> genData(String dataFile) throws IOException {
        List<String> stringList = GenData.genData4CourseInfoNew(Constant.AMOUNT, 2020, 5, 1);
        Records2File.checkRepeatlive(stringList);
        Records2File.asynchronousWrite(dataFile, stringList);
        return stringList;
    }

    public static void loadData4HBase(List<String> stringList, String yyyyMM) throws IOException {
        LOG.info(">>>>>>>> load data for HBase >>>>>>>");
        HBaseUtil.loadData(stringList, yyyyMM);
    }

    public static void loadData4Es(String indexName, String dataFile) throws IOException, InterruptedException {
        LOG.info(">>>>>>>> load data for ES >>>>>>>");
        LOG.info("indexName is [{}]", indexName);
        EsUtil.loadData(indexName, dataFile);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String yearMonth = "202005";
        String name = "x.course_info";
        String dataFile = "course_info.txt";
        //generate data
//        List<String> stringList = genData(dataFile);

        //load HBase data
        loadData4HBase(FileUtil.readFromFile(dataFile),yearMonth);

        //load ES
        String indexName = Tool.monthIndicesWithTable(yearMonth, Constant.SHARD_NUM, name).get(0);
        loadData4Es(indexName, dataFile);
    }
}
