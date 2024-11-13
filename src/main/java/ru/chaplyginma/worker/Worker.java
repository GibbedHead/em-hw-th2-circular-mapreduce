package ru.chaplyginma.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.chaplyginma.domain.KeyValue;
import ru.chaplyginma.manager.Manager;
import ru.chaplyginma.task.*;
import ru.chaplyginma.util.FileUtil;

import java.io.*;
import java.util.*;

public class Worker extends Thread {

    private static final String NAME_TEMPLATE = "Worker_%d";
    private static int id = 1;

    private final String workDir;
    private final Manager manager;
    private final Logger logger = LoggerFactory.getLogger(Worker.class);

    public Worker(Manager manager, String workDir) {
        this.workDir = workDir;
        this.setName(NAME_TEMPLATE.formatted(id++));
        this.manager = manager;
    }

    private static int getReduceTaskId(int numReduceTasks, KeyValue keyValue) {
        int reduceTaskId = 0;
        int hash = keyValue.key().hashCode();
        if (hash != Integer.MIN_VALUE) {
            reduceTaskId = Math.abs(hash) % numReduceTasks;
        }
        return reduceTaskId;
    }

    @Override
    public void run() throws IllegalStateException {
        logger.info("{} worker started", getName());

        while (manager.isWorking()) {
            try {
                Task task = manager.getTask();
                if (task != null) {
                    switch (task) {
                        case MapTask mapTask -> executeMap(mapTask);
                        case ReduceTask reduceTask -> executeReduce(reduceTask);
                        default ->
                                throw new IllegalStateException("Unexpected value type: " + task.getClass().getName());
                    }
                }
            } catch (InterruptedException e) {
                interrupt();
                logger.error("{} interrupted", getName());
            }
        }

        logger.info("{} worker stopped", getName());
    }

    private void executeMap(MapTask mapTask) {
        logger.info("{} starting map task {}", this.getName(), mapTask.getId());
        List<KeyValue> keyValues = map(mapTask.getFile());
        if (!keyValues.isEmpty()) {
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
            logger.error("{} error reading input file `{}`", this.getName(), file, e);
        }
        return result;
    }

    private void writeIntermediateFiles(List<KeyValue> keyValues,
                                        int taskId,
                                        int numReduceTasks,
                                        MapTaskResult mapTaskResult) {
        String taskDir = "%s/%s/%d".formatted(workDir, getName(), taskId);
        FileUtil.createDirectory(taskDir);

        Map<Integer, BufferedWriter> writers = new HashMap<>();

        try {
            for (KeyValue keyValue : keyValues) {
                int reduceTaskId = getReduceTaskId(numReduceTasks, keyValue);
                String fileName = "%s/mr-%d-%d.txt".formatted(taskDir, taskId, reduceTaskId);

                BufferedWriter writer = getWriter(writers, reduceTaskId, fileName);
                if (writer != null) {
                    writer.write(keyValue.toString());
                    writer.newLine();
                    mapTaskResult.files().add(fileName);
                }
            }
        } catch (IOException e) {
            logger.error("{} error writing intermediate files", getName(), e);
        } finally {
            closeWriters(writers);
        }
    }

    private BufferedWriter getWriter(Map<Integer, BufferedWriter> writers, int reduceTaskId, String fileName) {
        return writers.computeIfAbsent(reduceTaskId, k -> {
            try {
                return new BufferedWriter(new FileWriter(fileName, true));
            } catch (IOException e) {
                logger.error("{} error opening file {}", getName(), fileName, e);
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
                        logger.error("{} error closing writer for file: {}", getName(), e.getMessage());
                    }
                });
    }


    private void executeReduce(ReduceTask reduceTask) {
        logger.info("{} starting reduce task {}", this.getName(), reduceTask.getId());

        Map<String, List<String>> reduceMap = fillReduceMap(reduceTask);

        ReduceTaskResult result = new ReduceTaskResult(getResult(reduceMap));

        manager.completeReduceTask(reduceTask, result);
    }

    private Set<KeyValue> getResult(Map<String, List<String>> reduceMap) {
        Set<KeyValue> result = new HashSet<>();
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
            processFile(fileName, reduceMap);
        }
        return reduceMap;
    }

    private void processFile(String fileName, Map<String, List<String>> reduceMap) {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                processLine(reduceMap, line);
            }
        } catch (IOException e) {
            logger.error("{} error reading intermediate file {}", getName(), fileName, e);
        }
    }

    private void processLine(Map<String, List<String>> reduceMap, String line) {
        if (!line.isBlank()) {
            String[] words = line.split("\\t");
            if (words.length == 2) {
                reduceMap.computeIfAbsent(words[0], k -> new ArrayList<>()).add(words[1]);
            } else {
                System.out.printf("%s: Invalid line `%s`%n", getName(), line);
                logger.warn("{} invalid line `{}`", getName(), line);
            }
        }
    }

    private KeyValue reduce(String key, List<String> values) {
        return new KeyValue(key, String.valueOf(values.size()));
    }

}
