package ru.chaplyginma.worker;

import ru.chaplyginma.domain.KeyValue;
import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.task.MapTask;
import ru.chaplyginma.task.MapTaskResult;
import ru.chaplyginma.task.ReduceTask;
import ru.chaplyginma.task.Task;
import ru.chaplyginma.util.FileUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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
        try {
            Task task;
            task = manager.getNextMapTask(Thread.currentThread());
            while (task != null) {
                switch (task) {
                    case MapTask mapTask -> executeMap(mapTask);
                    case ReduceTask reduceTask -> executeReduce(reduceTask);
                    default -> throw new IllegalStateException("Unexpected value type: " + task.getClass().getName());
                }
                task = manager.getNextMapTask(Thread.currentThread());
            }
        } catch (InterruptedException e) {
            interrupt();
            System.out.println(getName() + " interrupted");
        }

    }

    private void executeMap(MapTask mapTask) {
        System.out.println(getName() + ": starting map task on file " + mapTask.getFile());
        List<KeyValue> keyValues = map(mapTask.getFile());
        if (keyValues != null) {
            System.out.println("Map result size for file " + mapTask.getFile() + ": " + keyValues.size());
            MapTaskResult mapTaskResult = new MapTaskResult(new HashSet<>());
            writeIntermediateFiles(keyValues, mapTask.getId(), mapTask.getNumReduceTasks(), mapTaskResult);
            manager.completeMapTask(mapTask, mapTaskResult);
        }
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

    private void writeIntermediateFiles(List<KeyValue> keyValues, int taskId, int numReduceTasks, MapTaskResult mapTaskResult) {
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
                mapTaskResult.files().add(fileName);
            } catch (IOException e) {
                System.err.println("Ошибка при записи в файл: " + e.getMessage());
            }
        }
    }

    private void executeReduce(ReduceTask reduceTask) {
        System.out.println(getName() + ": starting reduce task " + reduceTask);
    }


}
