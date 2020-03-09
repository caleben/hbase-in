import java.io.IOException;
import java.util.List;

/**
 * @author wenci 2020/3/7 15:50
 */
public class Main {
    public static void main(String[] args) throws IOException {
        HBaseUtil baseInput = new HBaseUtil();
        byte[][] splits = {"01".getBytes()};
//        baseInput.deleteTable(Constant.NAMESPACE, Constant.TABLE);
        baseInput.createTable(Constant.NAMESPACE, Constant.TABLE, splits);

        List<String> stringList = GenData.genData4CourseInfo(Constant.AMOUNT, 2);
        Records2File.checkRepeatlive(stringList);
        Records2File.asynchronousWrite("course_info.txt", stringList);
        baseInput.put(Constant.NAMESPACE, Constant.TABLE, stringList);
        baseInput.closeResource();
    }
}
