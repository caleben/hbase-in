import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author wenci 2020/3/8 19:25
 */
public class GenData {
    private static final Random RAND = new Random();

    public static List<String> genData4CourseInfo(int amount, int part) {
        ArrayList<String> list = new ArrayList<>();
        for (int i = 0; i < amount; i++) {
            String course = Constant.COURSE[RAND.nextInt(7)];
            String courseR = genRand(course);
            String teacherR = genRand(Constant.TEACHER[RAND.nextInt(7)]);
            String format = String.format("%02d", String2NumUtil.toNum(course) % part);
            String rowKey = format + course + suffix();
            list.add(rowKey + "\t" + courseR + "\t" + teacherR);
        }

        return list;
    }

    private static String genRand(String pre) {
        return pre + "_" + Constant.LETTER[RAND.nextInt(52)];
    }

    private static String suffix() {
        long nano = System.nanoTime();
        return String.valueOf(nano) + RAND.nextInt(1000);
    }

    public static void main(String[] args) {
        List<String> list = genData4CourseInfo(100, 2);
        list.forEach(System.out::println);
        System.out.println();
        Records2File.checkRepeatlive(list);
    }
}
