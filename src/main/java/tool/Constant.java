package tool;

/**
 * @author wenci 2020/3/7 7:19
 */
public class Constant {
    public static final Integer AMOUNT = 1_0000;
    public static final String NAMESPACE = "x";
    public static final String FAMILY = "f";
    public static final String TABLE = "course_info";
    public static final int SEGMENT = 2;
    public static final int HASH_NUM = 2;
    public static final int SHARD_NUM = 1;
    public static final String[] LETTER = new String[]{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
    public static final String[] COURSE = new String[]{"English", "Chinese", "Physical", "Chemistry", "Math", "History", "Political"};
    public static final String[] TEACHER = new String[]{"Tom", "Kong zi", "Einstein", "Harry potter", "Fourier", "Mr SiMa", "Napoleon"};
    public static final String[] COLUMNS = new String[]{"course", "teacher", "time"};

    public static boolean K8S = false;
    public static String ES_HBASE_HOST = "10.33.57.50";
    public static int ES_PORT = 9200;
    public static int HBASE_ZK_CLIENT_PORT = 2181;
    public static int BULK_ACTIONS = 1_000;
}
