import es.EsUtil;
import hbase.GenData;
import hbase.HBaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.Constant;
import tool.Records2File;

import java.io.IOException;
import java.util.List;

/**
 * @author wenci 2020/3/7 15:50
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static List<String> genData(String dataFile) throws IOException {
        List<String> stringList = GenData.genData4CourseInfo(Constant.AMOUNT, 2);
        Records2File.checkRepeatlive(stringList);
        Records2File.asynchronousWrite(dataFile, stringList);
        return stringList;
    }

    public static void loadData4HBase(List<String> stringList) throws IOException {
        LOG.info(">>>>>>>> load data for HBase >>>>>>>");
        HBaseUtil.loadData(stringList);
    }

    public static void loadData4Es(String indexName, String dataFile) throws IOException, InterruptedException {
        LOG.info(">>>>>>>> load data for ES >>>>>>>");
        EsUtil.loadData(indexName, dataFile);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //generate data
        String dataFile = "course_info.txt";
//        List<String> stringList = genData(dataFile);

        //load HBase data
//        loadData4HBase(stringList);

        //load ES
        loadData4Es("x.course_info", dataFile);
    }
}
