package tool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wenci 2020/4/22
 */
public class FileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);
    private static final String USER_DIR = System.getProperty("user.dir");

    public static List<String> readFromFile(String file) {
        List<String> collect = Collections.emptyList();
        String path = USER_DIR + File.separator + "data";
        LOG.info("data file path: {}", path);
        try {
            collect = Files.lines(Paths.get(path, file)).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return collect;
    }

    public static void main(String[] args) {
        readFromFile("course_info.txt");
    }
}
