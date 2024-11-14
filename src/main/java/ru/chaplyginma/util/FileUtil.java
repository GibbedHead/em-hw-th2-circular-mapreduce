package ru.chaplyginma.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Utility class for performing file and directory operations.
 *
 * <p>The {@code FileUtil} class provides static methods to create directories,
 * clear directories by deleting their contents, and manage (clear or create) directories.</p>
 */
public class FileUtil {

    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);

    private FileUtil() {
    }

    /**
     * Creates a directory at the specified path if it does not already exist.
     *
     * @param directoryPath the path of the directory to be created.
     */
    public static void createDirectory(String directoryPath) {
        File directory = new File(directoryPath);

        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (!created) {
                logger.error("Cannot create directory {}", directoryPath);
            }
        }
    }

    /**
     * Clears all contents of the specified directory, including subdirectories.
     *
     * @param directory the directory to be cleared.
     */
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

    /**
     * Manages the specified directory by clearing its contents if it exists,
     * or creating it if it does not.
     *
     * @param directoryName the name of the directory to manage.
     */
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
