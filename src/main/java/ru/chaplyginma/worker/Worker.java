package ru.chaplyginma.worker;

import ru.chaplyginma.domain.KeyValue;
import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.task.MapTask;
import ru.chaplyginma.task.MapTaskResult;
import ru.chaplyginma.task.ReduceTask;
import ru.chaplyginma.task.Task;
import ru.chaplyginma.util.FileUtil;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Worker extends Thread {

    private static final String NAME_TEMPLATE = "Worker_%d";
    private static final String RESULT_FILE_NAME = "results.txt";
    private static int id = 1;

    private final String workDir;
    private final Manager manager;

    public Worker(Manager manager, String workDir) {
        this.workDir = workDir;
        this.setName(NAME_TEMPLATE.formatted(id++));
        this.manager = manager;
    }

    @Override
    public void run() throws IllegalStateException {
        System.out.printf("%s: Started%n", this.getName());
        try {
            Task task;
            task = manager.getTask();
            while (task != null) {
                switch (task) {
                    case MapTask mapTask -> executeMap(mapTask);
                    case ReduceTask reduceTask -> executeReduce(reduceTask);
                    default -> throw new IllegalStateException("Unexpected value type: " + task.getClass().getName());
                }
                task = manager.getTask();
            }
        } catch (InterruptedException e) {
            interrupt();
            System.err.printf("%s: interrupted%n", this.getName());
        }
        System.out.printf("%s: Finished%n", this.getName());
    }

    private void executeMap(MapTask mapTask) {
        System.out.printf("%s: Starting map task %d%n", this.getName(), mapTask.getId());
        List<KeyValue> keyValues = map(mapTask.getFile());
        if (keyValues != null) {
            MapTaskResult mapTaskResult = new MapTaskResult(new HashSet<>());
            writeIntermediateFiles(keyValues, mapTask.getId(), mapTask.getNumReduceTasks(), mapTaskResult);
            manager.completeMapTask(mapTask, mapTaskResult, Thread.currentThread());
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
            System.err.printf("%s: Error reading input file `%s`: %s%n", this.getName(), file, e.getMessage());
            return null;
        }
        return result;
    }

    private void writeIntermediateFiles(List<KeyValue> keyValues, int taskId, int numReduceTasks, MapTaskResult mapTaskResult) {
        String taskDir = "%s/%s/%d".formatted(workDir, getName(), taskId);
        FileUtil.createDirectory(taskDir);

        Map<Integer, BufferedWriter> writers = new HashMap<>();

        try {
            for (KeyValue keyValue : keyValues) {
                int reduceTaskId = getReduceTaskId(numReduceTasks, keyValue);
                String fileName = "%s/mr-%d-%d.txt".formatted(taskDir, taskId, getReduceTaskId(numReduceTasks, keyValue));

                BufferedWriter writer = getWriter(writers, reduceTaskId, fileName);

                if (writer != null) {
                    writer.write(keyValue.toString());
                    writer.newLine();
                    mapTaskResult.files().add(fileName);
                }
            }
        } catch (IOException e) {
            System.out.printf("%s: Error write intermediate files `%s`: %s%n", getName(), taskDir, e.getMessage());
        } finally {
            closeWriters(writers);
        }
    }

    private static int getReduceTaskId(int numReduceTasks, KeyValue keyValue) {
        int reduceTaskId = 0;
        int hash = keyValue.key().hashCode();
        if (hash != Integer.MIN_VALUE) {
            reduceTaskId = Math.abs(hash) % numReduceTasks;
        }
        return reduceTaskId;
    }

    private BufferedWriter getWriter(Map<Integer, BufferedWriter> writers, int reduceTaskId, String fileName) {
        return writers.computeIfAbsent(reduceTaskId, k -> {
            try {
                return new BufferedWriter(new FileWriter(fileName, true));
            } catch (IOException e) {
                System.err.printf("%s: Error opening file `%s`: %s%n ", getName(), fileName, e.getMessage());
                return null;
            }
        });
    }

    private void closeWriters(Map<Integer, BufferedWriter> writers) {
        writers.values().stream()
                .filter(Objects::nonNull)
                .forEach(writer -> {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        System.err.printf("%s: Error closing writer for file: %s%n", getName(), e.getMessage());
                    }
                });
    }


    private void executeReduce(ReduceTask reduceTask) {
        System.out.printf("%s: Starting reduce task %d%n", this.getName(), reduceTask.getId());

        Map<String, List<String>> reduceMap = fillReduceMap(reduceTask);

        List<String> result = getResult(reduceMap);

        writeResultFile(result);

        manager.completeReduceTask(reduceTask, Thread.currentThread());
    }

    private List<String> getResult(Map<String, List<String>> reduceMap) {
        List<String> result = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : reduceMap.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            result.add(reduce(key, values));
        }
        return result;
    }

    private Map<String, List<String>> fillReduceMap(ReduceTask reduceTask) {
        Map<String, List<String>> reduceMap = new HashMap<>();
        for (String fileName : reduceTask.getFiles()) {
            try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.isBlank()) {
                        String[] words = line.split("\\t");
                        if (words.length == 2) {
                            reduceMap.computeIfAbsent(words[0], k -> new ArrayList<>()).add(words[1]);
                        } else {
                            System.out.printf("%s: Invalid line `%s`", getName(), line);
                        }
                    }
                }
            } catch (IOException e) {
                System.err.printf("%s: Error reading file `%s`: %s%n", getName(), fileName, e.getMessage());
            }
        }
        return reduceMap;
    }

    private String reduce(String key, List<String> values) {
        return "%s\t%d".formatted(key, values.size());
    }

    private void writeResultFile(List<String> result) {
        String fileName = "%s/%s".formatted(workDir, RESULT_FILE_NAME);
        Path filePath = Paths.get(fileName);

        try {
            Files.write(filePath, result);
        } catch (IOException e) {
            System.err.printf("%s: Error writing result file `%s`: %s%n", getName(), fileName, e.getMessage());
        }
    }
}
