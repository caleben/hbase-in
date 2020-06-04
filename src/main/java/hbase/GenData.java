package hbase;

import tool.Constant;
import tool.Records2File;
import tool.String2NumUtil;
import tool.Tool;

import java.io.IOException;
import java.time.YearMonth;
import java.time.ZoneOffset;
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

    public static List<String> genData4CourseInfoNew(int amount, int beginYear, int beginMonth, int totalMonth) {
        List<String> list = new ArrayList<>();
        YearMonth begin = YearMonth.of(beginYear, beginMonth);
        long time;
        for (int i = 0; i < amount; i++) {
            time = getRandTime(begin, totalMonth);
            String course = Constant.COURSE[RAND.nextInt(7)];
            String courseR = genRand(course);
            String teacherR = genRand(Constant.TEACHER[RAND.nextInt(7)]);
            String rowKey = Tool.produceDynamicRowKey(time, Constant.SEGMENT, Constant.HASH_NUM, courseR);
            list.add(rowKey + "\t" + courseR + "\t" + teacherR + "\t" + time);
        }
        return list;
    }

    public static long getRandTime(YearMonth begin, int totalMonth) {
        long start = begin.atDay(1).atStartOfDay().toEpochSecond(ZoneOffset.of("+8")) * 1000;
        long end = begin.plusMonths(totalMonth).atDay(1).atStartOfDay().toEpochSecond(ZoneOffset.of("+8")) * 1000;

        long range = (end- start);
        return start + (long) (Math.random() * range);
    }

    public static void main(String[] args) throws IOException {
        List<String> list = genData4CourseInfoNew(1000, 2020, 5, 1);
        list.forEach(System.out::println);
        System.out.println();
        Records2File.checkRepeatlive(list);
    }
}
