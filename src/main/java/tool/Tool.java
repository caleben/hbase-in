package tool;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * @author wuenci
 * @date 2020/6/3 18:56
 */
public class Tool {
    private static DateTimeFormatter yyyyMm = DateTimeFormatter.ofPattern("yyyyMM");
    private static Pattern HUMAN_INFO_ROWKEY_REGEX = Pattern.compile("^[0-9]{4}_[0-9a-zA-Z`~!@#$%^&*()_\\-+={}\\[\\]|\\\\:;\"'<,>.?/]+$");
    private static String HUMAN_ID_REG = "^\\d{4}_.+";

    public static String LINE = "-";
    public static String WAVY = "~";

    private static String hashFormatAndInsert(String src, int rang, int start, String pattern, int offset) {
        return insert(offset, String.format(pattern, hash(src, rang, start)), src);
    }

    private static String insert(int offset, String s, String src) {
        StringBuilder sb = new StringBuilder(src);
        sb.insert(offset, s);
        return sb.toString();
    }

    private static int hash(String src, int rang, int start) {
        return Math.abs(src.hashCode() % rang) + 1 + start;
    }

    /**
     * 获取静态表rowKey前缀
     * @param total 总散列仁
     * @param step 散列间隔
     * @return list like: List{"0001","0034","0067",...}
     */
    public static List<String> staticPrefix(int total, int step) {
        assertNum(total, step);
        List<String> list = new ArrayList<>(64);
        list.add(formatStrFixedLen(1, 4));
        int y;
        for (int i = 1; (y = i + step) < total; ) {
            list.add(formatStrFixedLen(y, 4));
            i = y;
        }
        list.add(formatStrFixedLen(total, 4));
        //默认
        return list;
    }

    /**
     * 获取静态表所有Indices
     * @param total 总散列仁
     * @param step 散列间隔
     * @param schemaName schema
     * @param tableName table
     * @return like: List{"xface.face_similar_check-0001-0034","xface.face_similar_check-0034-0067",...}
     */
    public static List<String> indexStaticTable(int total, int step, String schemaName, String tableName) {
        assertNum(total, step);
        String indexPrefix = (schemaName + "." + tableName + "-").toLowerCase(Locale.US);
        List<String> list = new ArrayList<>(64);
        list.add(formatStrFixedLen(1, 4));
        int y;
        for (int i = 1; (y = i + step) < total; ) {
            list.add(indexPrefix + formatStrFixedLen(i, 4) + WAVY + formatStrFixedLen(y, 4));
            i = y;
        }

        list.add(indexPrefix + formatStrFixedLen(y - step, 4) + WAVY + formatStrFixedLen(total, 4));
        //默认
        return list;
    }

    static void assertNum(int a, int b) {
        if (a <= 0 || b <= 0) {
            throw new IllegalArgumentException("number must be > 0");
        }
    }

    /**
     * 动态表rowKey生成
     * 规则：yyyyMM + 4位段 + 2位hash + 其他列
     * 其中段是由1开始如0100,0200,0300,0400
     * hash是由0开始如 00,01
     *
     * @param timeStamp  timestamp
     * @param segment 表段值
     * @param hashNum 表散列值
     * @param hashFieldValues hash字段值
     * @return rowKey
     */
    public static String produceDynamicRowKey(long timeStamp, int segment, int hashNum,
                                              Object... hashFieldValues) {
        assertTime(timeStamp);
        Instant instant = Instant.ofEpochMilli(timeStamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        String yyyyMM = String.format("%04d%02d", dateTime.getYear(), dateTime.getMonthValue());

        int hashValue = 0;
        if (hashFieldValues != null && hashFieldValues.length > 0) {
            StringBuilder builder = new StringBuilder();
            for (Object hashFieldValue : hashFieldValues) {
                builder.append(hashFieldValue);
            }
            hashValue = Math.abs(builder.toString().hashCode()) % hashNum;
        }

        String rowKey = yyyyMM +
                segmentStr(timeStamp, segment) +
                formatStrFixedLen(hashValue, 2) +
                getUuid();

        return rowKey;
    }

    public static List<String> produceSplits(String yearMonth, int segment, int hashNum) {
        List<String> list = new ArrayList<>(16);
        String seg, hash;
        for (int i = 1; i <= segment; i++) {
            seg = String.format("%02d00", i);
            for (int j = 0; j < hashNum; j++) {
                hash = String.format("%02d", j);
                list.add(yearMonth + seg + hash);
            }
        }
        return list;
    }

    public static String getUuid() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString().replace("-", "");
    }

    public static String segmentStr(long timeStamp, int numSegmentPerMonth) {
        return formatStrFixedLen(segmentCompute(timeStamp, numSegmentPerMonth), 2) + "00";
    }

    public static List<String> segmentBetweenTimeRange(long startInclude, long endInclude, int numSegmentPerMonth) {
        assertTime(startInclude);
        assertTime(endInclude);
        ArrayList<String> list = new ArrayList<>(16);
        LocalDateTime startDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(startInclude), ZoneId.systemDefault());
        LocalDateTime endDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(endInclude), ZoneId.systemDefault());
        YearMonth from = YearMonth.of(startDate.getYear(), startDate.getMonth());
        YearMonth to = YearMonth.of(endDate.getYear(), endDate.getMonth());

        int startDay = startDate.getDayOfMonth();
        int endDay = endDate.getDayOfMonth();
        int days = from.lengthOfMonth();
        int n = days / numSegmentPerMonth;
        int y = days % numSegmentPerMonth;
        int distance, offset = 1;
        int start, end;
        if (from.equals(to)) {
            //算出start与end所在年月，若相同则算出落在的索引
            for (int i = 0; i < numSegmentPerMonth; i++) {
                distance = i < y ? n + 1 : n;
                start = offset;
                end = offset + distance - 1;
                if (startDay <= end) {
                    if (endDay >= start) {
                        list.add(formatSegment(from, i));
                    } else {
                        break;
                    }
                }
                offset += distance;
            }
        } else {
            //若不同则加入头尾落在的索引及中间月所有索引
            //头
            list.addAll(segmentInMonthPoint(startInclude, numSegmentPerMonth, false));
            //中间
            YearMonth step = from;
            while ((step = step.plusMonths(1)).compareTo(to) < 0) {
                list.addAll(monthSegments(step, numSegmentPerMonth));
            }
            //尾
            list.addAll(segmentInMonthPoint(endInclude, numSegmentPerMonth, true));
        }

        return list;
    }

    public static List<String> segmentInMonthPoint(long timeStamp, int numSegmentPerMonth, boolean before) {
        assertTime(timeStamp);
        ArrayList<String> list = new ArrayList<>(8);
        Instant instant = Instant.ofEpochMilli(timeStamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        int day = dateTime.getDayOfMonth();
        YearMonth yearMonth = YearMonth.of(dateTime.getYear(), dateTime.getMonth());
        int days = yearMonth.lengthOfMonth();
        int n = days / numSegmentPerMonth;
        int y = days % numSegmentPerMonth;
        int distance, offset = 1;
        int start, end;
        for (int i = 0; i < numSegmentPerMonth; i++) {
            distance = i < y ? n + 1 : n;
            start = offset;
            end = offset + distance - 1;
            if (before) {
                if (day >= start) {
                    list.add(formatSegment(yearMonth, i));
                }
            } else {
                if (day <= end) {
                    list.add(formatSegment(yearMonth, i));
                }
            }
            offset += distance;
        }
        return list;
    }

    public static String formatSegment(YearMonth yearMonth, int i) {
        return String.format("%04d%02d", yearMonth.getYear(), yearMonth.getMonthValue()) + formatStrFixedLen(i + 1, 2) + "00";
    }

    public static List<String> monthSegments(YearMonth yearMonth, int numSegmentPerMonth) {
        List<String> list = new ArrayList<>(16);
        for (int i = 0; i < numSegmentPerMonth; i++) {
            list.add(formatSegment(yearMonth, i));
        }

        return list;
    }

    /**
     * 计算时间戳对应的区间，如果
     *
     * @param timeStamp 13垃时间戳
     * @param numSegmentPerMonth the number segments per Month
     * @return int
     */
    private static int segmentCompute(long timeStamp, int numSegmentPerMonth) {
        assertTime(timeStamp);
        Instant instant = Instant.ofEpochMilli(timeStamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        int day = dateTime.getDayOfMonth();
        YearMonth yearMonth = YearMonth.of(dateTime.getYear(), dateTime.getMonth());
        int days = yearMonth.lengthOfMonth();
        int n = days / numSegmentPerMonth;
        int y = days % numSegmentPerMonth;
        int distance, offset = 1;
        int end;
        for (int i = 0; i < numSegmentPerMonth; i++) {
            distance = i < y ? n + 1 : n;
            end = offset + distance - 1;
            if (day <= end) {
                return i + 1;
            }
            offset += distance;
        }
        return 1;
    }

    /**
     * 计算输入的时间戳落在哪个index中
     *
     * @param timeStamp 13位时间戳,精确到毫秒
     * @param numIndexPerMonth the number indexes per Month
     * @return string, like xvehicle.bayonet_vehiclepass-202004-01~10
     */
    public static String indexComputeWithTable(long timeStamp, int numIndexPerMonth, String tableName) {
        assertTime(timeStamp);
        Instant instant = Instant.ofEpochMilli(timeStamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        int day = dateTime.getDayOfMonth();
        YearMonth yearMonth = YearMonth.of(dateTime.getYear(), dateTime.getMonth());
        int days = yearMonth.lengthOfMonth();
        int n = days / numIndexPerMonth;
        int y = days % numIndexPerMonth;
        int distance, offset = 1;
        int start, end;
        for (int i = 0; i < numIndexPerMonth; i++) {
            distance = i < y ? n + 1 : n;
            start = offset;
            end = offset + distance - 1;
            if (day <= end) {
                return formatIndexWithTable(tableName, yearMonth, start, end);
            }
            offset += distance;
        }
        return "";
    }

    /**
     * 计算本月在给定时间点之前的索引或之后的索引
     *
     * @param timeStamp 13位时间戳,精确到毫秒
     * @param numIndexPerMonth the number indexes per Month
     * @param tableName 表名
     * @param before 是否包含本月之前的索引，还是包含之后的索引
     * @return list
     */
    public static List<String> indexInMonthPoint(long timeStamp, int numIndexPerMonth, String tableName, boolean before) {
        assertTime(timeStamp);
        ArrayList<String> list = new ArrayList<>(8);
        Instant instant = Instant.ofEpochMilli(timeStamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        int day = dateTime.getDayOfMonth();
        YearMonth yearMonth = YearMonth.of(dateTime.getYear(), dateTime.getMonth());
        int days = yearMonth.lengthOfMonth();
        int n = days / numIndexPerMonth;
        int y = days % numIndexPerMonth;
        int distance, offset = 1;
        int start, end;
        for (int i = 0; i < numIndexPerMonth; i++) {
            distance = i < y ? n + 1 : n;
            start = offset;
            end = offset + distance - 1;
            if (before) {
                if (day >= start) {
                    list.add(formatIndexWithTable(tableName, yearMonth, start, end));
                }
            } else {
                if (day <= end) {
                    list.add(formatIndexWithTable(tableName, yearMonth, start, end));
                }
            }
            offset += distance;
        }
        return list;
    }

    /**
     * 计算一段时间区间内的所有索引
     *
     * @param startInclude 13位开始时间
     * @param endInclude 13位结束时间
     * @param numIndexPerMonth the number indexes per Month
     * @param tableName 表名
     * @return list
     */
    public static List<String> indexBetweenTimeRange(long startInclude, long endInclude, int numIndexPerMonth, String tableName) {
        assertTime(startInclude);
        assertTime(endInclude);
        ArrayList<String> list = new ArrayList<>(16);
        LocalDateTime startDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(startInclude), ZoneId.systemDefault());
        LocalDateTime endDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(endInclude), ZoneId.systemDefault());
        YearMonth from = YearMonth.of(startDate.getYear(), startDate.getMonth());
        YearMonth to = YearMonth.of(endDate.getYear(), endDate.getMonth());

        int startDay = startDate.getDayOfMonth();
        int endDay = endDate.getDayOfMonth();
        int days = from.lengthOfMonth();
        int n = days / numIndexPerMonth;
        int y = days % numIndexPerMonth;
        int distance, offset = 1;
        int start, end;
        if (from.equals(to)) {
            //算出start与end所在年月，若相同则算出落在的索引
            for (int i = 0; i < numIndexPerMonth; i++) {
                distance = i < y ? n + 1 : n;
                start = offset;
                end = offset + distance - 1;
                if (startDay <= end) {
                    if (endDay >= start) {
                        list.add(formatIndexWithTable(tableName, from, start, end));
                    } else {
                        break;
                    }
                }
                offset += distance;
            }
        } else {
            //若不同则加入头尾落在的索引及中间月所有索引
            //头
            list.addAll(indexInMonthPoint(startInclude, numIndexPerMonth, tableName, false));
            //中间
            YearMonth step = from;
            while ((step = step.plusMonths(1)).compareTo(to) < 0) {
                list.addAll(monthIndicesWithTable(step, numIndexPerMonth, tableName));
            }
            //尾
            list.addAll(indexInMonthPoint(endInclude, numIndexPerMonth, tableName, true));
        }

        return list;
    }

    private static String formatIndexWithTable(String tableName, YearMonth yearMonth, int start, int end) {
        return (tableName.isEmpty() ? tableName : tableName + LINE) +
                String.format("%04d%02d", yearMonth.getYear(), yearMonth.getMonthValue()) +
                LINE + String.format("%02d", start) + WAVY + String.format("%02d", end);
    }

    /**
     * 计算输入的时间戳落在哪个index中, 只是时间后缀
     *
     * @param timeStamp 13位时间戳,精确到毫秒
     * @param numIndexPerMonth the number indexes per Month
     * @return string, like 202004-01~10
     */
    static String indexCompute(long timeStamp, int numIndexPerMonth) {
        return indexComputeWithTable(timeStamp, numIndexPerMonth, "");
    }

    /**
     * 计算单月索引划分包含tableName
     *
     * @param yearMonth like "yyyyMM"
     * @param numIndexPerMonth the number indexes per Month
     * @param tableName the prefix, tableName
     * @return list, like List{xvehicle.bayonet_vehiclepass-202004-01~10, xvehicle.bayonet_vehiclepass-202004-11~20,
     *                         xvehicle.bayonet_vehiclepass-202004-21~30}
     */
    public static List<String> monthIndicesWithTable(String yearMonth, int numIndexPerMonth, String tableName) {
        YearMonth parse = YearMonth.parse(yearMonth, yyyyMm);
        return monthIndicesWithTable(parse, numIndexPerMonth, tableName);
    }

    public static List<String> monthIndicesWithTable(YearMonth yearMonth, int numIndexPerMonth, String tableName) {
        int days = yearMonth.lengthOfMonth();
        int n = days / numIndexPerMonth;
        int y = days % numIndexPerMonth;
        int distance, offset = 1;
        String start, end;
        List<String> list = new ArrayList<>(16);
        for (int i = 0; i < numIndexPerMonth; i++) {
            distance = i < y ? n + 1 : n;
            start = String.format("%02d", offset);
            end = String.format("%02d", offset + distance - 1);
            list.add((tableName.isEmpty() ? tableName : tableName + LINE) +
                    String.format("%04d%02d", yearMonth.getYear(), yearMonth.getMonthValue()) +
                    LINE + start + WAVY + end);
            offset += distance;
        }
        return list;
    }

    /**
     * 计算单月索引划分
     *
     * @param yearMonth like "yyyyMM"
     * @param numIndexPerMonth the number indexes per Month
     * @return list, like List{202004-01~10, 202004-11~20, 202004-21~30}
     */
    public static List<String> monthIndices(String yearMonth, int numIndexPerMonth) {
        return monthIndicesWithTable(yearMonth, numIndexPerMonth, "");
    }

    /**
     * 将value格式化特定长度的字符串，若本身长度大于指定长度则直接返回本身
     *
     * @param value 输入int值
     * @param length 需要格式化长度
     * @return string
     */
    public static String formatStrFixedLen(int value, int length) {
        if (String.valueOf(value).length() >= length) {
            return String.valueOf(value);
        }

        return String.format("%0" + length + "d", value);
    }

    /**
     * 确保时间戳13位
     *
     * @param timeStamp 时间戳
     */
    public static void assertTime(long timeStamp) {
        if (13 != String.valueOf(timeStamp).length()) {
            throw new IllegalArgumentException("ts field is not 13 length");
        }
    }

    public static void main(String[] args) {
//        staticPrefix(1000, 33).forEach(System.out::println);
        indexStaticTable(1000, 33, "xface", "face_similar_check").forEach(System.out::println);
//        System.exit(0);
//        String s = hashFormatAndInsert("_111_01459be0-d952-4377-8f86-55be0a7d8cb832576", 999, 0, "%04d", 0);
//        System.out.println(s);


        //2020-03-12
        long l16 = 1583942400000L;
        //2020-05-16
        long l17 = 1589558400000L;

        List<String> l = segmentBetweenTimeRange(l16, l17, 4);
        System.out.println(l);
        indexBetweenTimeRange(l16, l17, 1, "xface.snap_image_info").forEach(System.out::println);

//        List<String> list = monthIndices("202005", 4);
//        System.out.println(list);
//        String index = indexCompute(l17, 4);
//
//        System.out.println(index);
    }
}
