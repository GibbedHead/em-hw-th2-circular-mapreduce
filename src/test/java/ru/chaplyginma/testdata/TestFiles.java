package ru.chaplyginma.testdata;

import org.instancio.Instancio;
import ru.chaplyginma.util.FileUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class TestFiles {

    private static final Random RANDOM = new Random();

    private TestFiles() {
    }

    public static Set<String> makeTestFiles(String workDir, int numFiles) {
        FileUtil.manageDirectory(workDir);

        return createTestFiles(workDir, numFiles);
    }

    private static Set<String> createTestFiles(String workDir, int numFiles) {
        Set<String> testFiles = new HashSet<>();

        for (int i = 0; i < numFiles; i++) {
            String fileName = "%s/test-file-%d.txt".formatted(workDir, i);
            Path filePath = Paths.get(fileName);
            List<String> data = getContent();
            try {
                Files.write(filePath, data);
                testFiles.add(fileName);
            } catch (IOException e) {
                System.out.printf("Error writing test file: %s%n", fileName);
            }
        }

        return testFiles;
    }

    private static List<String> getContent() {
        return Instancio.gen().text().loremIpsum().list(5 + RANDOM.nextInt(5));
    }


}
