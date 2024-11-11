package ru.chaplyginma.util;

import java.io.File;

public class FileUtil {

    public static void createDirectory(String directoryPath) {
        File directory = new File(directoryPath);

        if (!directory.exists()) {
            // Если директория не существует, создаём её
            boolean created = directory.mkdirs();
            if (created) {
                System.out.println("Директория " + directoryPath + " успешно создана.");
            } else {
                System.out.println("Не удалось создать директорию " + directoryPath);
            }
        }
    }

    public static void clearDirectory(File directory) {
        // Получаем все файлы и поддиректории
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    clearDirectory(file); // Рекурсивно очищаем поддиректории
                }
                // Удаляем файл или пустую директорию
                boolean deleted = file.delete();
                if (deleted) {
                    System.out.println("Удалён: " + file.getAbsolutePath());
                } else {
                    System.out.println("Не удалось удалить: " + file.getAbsolutePath());
                }
            }
        }
    }
}
