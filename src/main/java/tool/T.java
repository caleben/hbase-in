package tool;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author wuenci
 * @date 2020/6/16 11:28
 * @since 2.0.0
 */
public class T {
    private static <T> void rotate1(List<T> list, int distance) {
        int size = list.size();
        if (size == 0) {
            return;
        }
        distance = distance % size;
        if (distance < 0) {
            distance += size;
        }
        if (distance == 0) {
            return;
        }

        for (int cycleStart = 0, nMoved = 0; nMoved != size; cycleStart++) {
            T displaced = list.get(cycleStart);
            int i = cycleStart;
            do {
                i += distance;
                if (i >= size) {
                    i -= size;
                }
                displaced = list.set(i, displaced);
                nMoved ++;
            } while (i != cycleStart);
        }
    }
    public static void main(String[] args) {
        List<String> collect = Stream.of("a", "b", "c", "d").collect(Collectors.toList());
        rotate1(collect, 1);
        System.out.println(collect);
    }
}
