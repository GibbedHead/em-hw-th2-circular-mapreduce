package ru.chaplyginma.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);

    private FileUtil() {
    }

    public static void createDirectory(String directoryPath) {
        File directory = new File(directoryPath);

        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                logger.error("Cannot create directory {}", directoryPath);
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
                    logger.error("Cannot delete file {}", file.getAbsolutePath());
                }
            }
        }
    }

    public static void manageDirectory(String directoryName) {
        File directory = new File(directoryName);
        if (directory.exists()) {
            clearDirectory(directory);
        } else {
            boolean created = directory.mkdirs();
            if (!created) {
                logger.error("Cannot temp create directory {}", directoryName);
            }
        }
    }
}
