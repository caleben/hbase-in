import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * @author wenci 2020/3/7 15:55
 */
public class String2NumUtil {
    private final static Pattern NUM_PATTERN = Pattern.compile("\\d+");

    public static int toNum(String str) {
        requireNonNull(str);
        if (isNum(str)) {
            return Integer.parseInt(str);
        }

        char[] chars = prune(str, 9).toCharArray();
        StringBuilder sb = new StringBuilder();
        for (char aChar : chars) {
            if (charIsNum(aChar)) {
                sb.append(aChar);
            } else {
                sb.append((int) aChar);
            }
        }
        return Integer.parseInt(prune(sb.toString(), 9));
    }

    public static String nextStr(String str) {
        requireNonNull(str);
        return str.substring(0, str.length() - 1) + (char) (str.charAt(str.length() - 1) + 1);
    }

    private static String prune(String input, int retainSize) {
        requireNonNull(input);
        String ret = input.trim();
        if (ret.length() > retainSize) {
            ret = ret.substring(ret.length() - retainSize);
            return ret;
        }
        return ret;
    }

    private static boolean isNum(String str) {
        return NUM_PATTERN.matcher(str).matches();
    }

    private static boolean charIsNum(char a) {
        return a >= 48 && a <= 57;
    }

    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE);
        String c = "10 9a ";
        String e = "10 9abesfaf8";
        System.out.println(c);
        System.out.println(nextStr(c));
        System.out.println(toNum(c));
        System.out.println(toNum(e));
    }
}
