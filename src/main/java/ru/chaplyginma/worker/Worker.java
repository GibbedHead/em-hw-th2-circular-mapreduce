package ru.chaplyginma.worker;

import ru.chaplyginma.domain.KeyValue;
import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.task.MapTask;
import ru.chaplyginma.util.FileUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Worker extends Thread {

    private static final String NAME_TEMPLATE = "Worker_%d";
    private static int id = 1;

    private final String workDir;
    private final Manager manager;

    public Worker(Manager manager, String workDir) {
        this.workDir = workDir;
        this.setName(NAME_TEMPLATE.formatted(id++));
        this.manager = manager;
    }

    @Override
    public void run() {
        MapTask mapTask;
        try {
            mapTask = manager.getNextMapTask(Thread.currentThread());
            while (mapTask != null) {
                System.out.println(getName() + ": starting map task on file " + mapTask.getFile());
                List<KeyValue> keyValues = map(mapTask.getFile());
                if (keyValues != null) {
                    System.out.println("Map result size for file " + mapTask.getFile() + ": " + keyValues.size());
                    writeIntermediateFiles(keyValues, mapTask.getId(), mapTask.getNumReduceTasks());
                    manager.completeMapTask(mapTask, "MapTask: " + mapTask.getId() + " completed");
                }
                mapTask = manager.getNextMapTask(Thread.currentThread());
            }
        } catch (InterruptedException e) {
            interrupt();
            System.out.println(getName() + " interrupted");
        }
        System.out.println(getName() + " finished");
    }

    private List<KeyValue> map(String file) {
        List<KeyValue> result = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                for (String word : line.split("\\P{L}+")) {
                    if (!word.isBlank()) {
                        result.add(new KeyValue(word.toLowerCase(), "1"));
                    }
                }
            }
        } catch (IOException e) {
            System.err.printf("Error reading input file `%s`: %s%n", file, e.getMessage());
            return null;
        }
        return result;
    }

    private void writeIntermediateFiles(List<KeyValue> keyValues, int taskId, int numReduceTasks) {
        String taskDir = "%s/%s/%d".formatted(workDir, getName(), taskId);
        FileUtil.createDirectory(taskDir);
        for (KeyValue keyValue : keyValues) {
            int reduceTaskId = 0;
            int hash = keyValue.key().hashCode();
            if (hash != Integer.MIN_VALUE) {
                reduceTaskId = Math.abs(hash) % numReduceTasks;
            }
            String fileName = "%s/mr-%d-%d.txt".formatted(taskDir, taskId, reduceTaskId);
            try (FileWriter fileWriter = new FileWriter(fileName, true)) { // 'true' означает, что мы добавляем текст
                String text = keyValue + "\n";
                fileWriter.write(text); // Записываем текст в файл
            } catch (IOException e) {
                System.err.println("Ошибка при записи в файл: " + e.getMessage());
            }
        }
    }


}
