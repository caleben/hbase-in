package tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author wenci 2020/3/9 10:24
 */
public class Records2File {
    private static final Logger LOG = LoggerFactory.getLogger(Records2File.class);
    private static final ExecutorService pool = Executors.newFixedThreadPool(1);
    private static final String DIR = System.getProperty("user.dir") + File.separator + "data";

    public Records2File() {
    }

    public static void asynchronousWrite(String fileNameWithoutPath, List<String> src) {
        checkFileValid(fileNameWithoutPath);

        Thread task = new Thread(() -> {
            int count = 0;
            try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(DIR + File.separator + fileNameWithoutPath), StandardOpenOption.APPEND)) {
                Instant start = Instant.now();
                for (String s : src) {
                    bw.write(s);
                    count++;
                    bw.newLine();
                }
                bw.flush();
                LOG.info("write done, cost:{}ms", ChronoUnit.MILLIS.between(start, Instant.now()));
            } catch (IOException e) {
                throw new RuntimeException("write file error! already write:" + count, e);
            }
        });
        task.start();
    }

    private static void checkFileValid(String fileNameWithoutPath) {
        File dir = new File(DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File file = new File(dir, fileNameWithoutPath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException("check file valid false, fileName:" + fileNameWithoutPath, e);
            }
        }
    }

    public static Map<String, Long> checkRepeat(String fileNameWithoutPath) {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(DIR + File.separator + fileNameWithoutPath))) {
            Map<String, Long> collect = reader.lines().collect(Collectors.groupingBy(line -> line.split("\t")[0], Collectors.counting()));
            Map<String, Long> result = collect.entrySet().stream().filter(entry -> entry.getValue() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            LOG.info("repeat :{}", result);
            return result;
        } catch (IOException e) {
            throw new RuntimeException("check repeat error!", e);
        }
    }

    public static Map<String, Long> checkRepeatlive(List<String> stringList) throws IOException {
        Map<String, Long> collect = stringList.stream().collect(Collectors.groupingBy(line -> line.split("\t")[0], Collectors.counting()));
        Map<String, Long> result = collect.entrySet().stream().filter(entry -> entry.getValue() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (result.size() > 0) {
            throw new IOException("rowKey is redundancy,repeat: " + result);
        }
        return result;
    }
}
