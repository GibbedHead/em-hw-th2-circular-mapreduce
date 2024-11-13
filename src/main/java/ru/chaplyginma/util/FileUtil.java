package ru.chaplyginma.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileUtil {

    private FileUtil() {
    }

    public static void createDirectory(String directoryPath) {
        File directory = new File(directoryPath);

        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                System.out.println("Не удалось создать директорию " + directoryPath);
            }
        }
    }

    public static void clearDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    clearDirectory(file);
                }
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    System.out.println("Не удалось удалить: " + file.getAbsolutePath());
                }
            }
        }
    }
}
