package ru.chaplyginma.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.chaplyginma.domain.KeyValue;
import ru.chaplyginma.task.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class Manager extends Thread {

    private static final int WORKER_TIMEOUT_MILLISECONDS = 200;

    private final Set<String> files;
    private final int numReduceTasks;
    private final String workDir;
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final Map<Task, ScheduledFuture<?>> timeoutFutures = new ConcurrentHashMap<>();
    private final CountDownLatch mapLatch;
    private final CountDownLatch reduceLatch;
    private final Lock completeTasklock = new ReentrantLock();
    private final Map<Task, MapTaskResult> mapResults = new ConcurrentHashMap<>();
    private final Map<Task, ReduceTaskResult> reduceResults = new ConcurrentHashMap<>();
    private final Logger logger = LoggerFactory.getLogger(Manager.class);
    private volatile boolean working = true;

    public Manager(Set<String> files, int numReduceTasks, String workDir) {
        this.files = files;
        this.workDir = workDir;
        this.numReduceTasks = numReduceTasks;

        this.mapLatch = new CountDownLatch(files.size());
        this.reduceLatch = new CountDownLatch(numReduceTasks);
    }

    @Override
    public void run() {
        logger.info("Started");

        createMapTasks();
        waitForMapTasksCompletion();
        logger.info("Map tasks completed");

        clearQueues();

        createReduceTasks();
        waitForReduceTasksCompletion();
        logger.info("Reduce tasks completed");

        writeResult();

        clearQueues();

        finish();
        logger.info("Finished.");
    }

    public Task getTask() throws InterruptedException {
        final Task task = taskQueue.poll(1, TimeUnit.MILLISECONDS);

        if (task == null) {
            return null;
        }

        if (isTaskCompleted(task)) {
            return null;
        }

        initializeTask(task);
        return task;
    }

    public void completeMapTask(MapTask task, MapTaskResult mapTaskResult) {
        completeTask(task, mapTaskResult, mapResults, mapLatch, "map");
    }

    public void completeReduceTask(ReduceTask task, ReduceTaskResult result) {
        completeTask(task, result, reduceResults, reduceLatch, "reduce");
    }

    public boolean isWorking() {
        return working;
    }

    private boolean isTaskCompleted(Task task) {
        return mapResults.containsKey(task) || reduceResults.containsKey(task);
    }

    private void initializeTask(Task task) {
        task.start();
        scheduleTimeout(task);
    }

    private <T> void completeTask(Task task,
                                  T taskResult,
                                  Map<Task, T> resultMap,
                                  CountDownLatch latch,
                                  String taskType) {
        completeTasklock.lock();
        try {
            if (resultMap.containsKey(task)) {
                return;
            }

            ScheduledFuture<?> scheduledFuture = timeoutFutures.remove(task);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }

            resultMap.put(task, taskResult);
            latch.countDown();
            logger.info("Completed {} task {} ", taskType, task.getId());
        } finally {
            completeTasklock.unlock();
        }
    }

    private void createMapTasks() {
        for (String file : files) {
            taskQueue.add(new MapTask(file, numReduceTasks));
        }
        logger.info("Map tasks created");
    }

    private void createReduceTasks() {
        Task.resetGenerator();
        for (int i = 0; i < numReduceTasks; i++) {
            taskQueue.add(new ReduceTask(getMapFilesByReduceId(i)));
        }
        logger.info("Reduce tasks created");
    }

    private Set<String> getMapFilesByReduceId(int reduceId) {
        String reduceIdPattern = "-%d.txt".formatted(reduceId);
        return mapResults.values().stream()
                .flatMap(mapTaskResult -> mapTaskResult.files().stream())
                .filter(fileName -> fileName.endsWith(reduceIdPattern))
                .collect(Collectors.toSet());
    }

    private void scheduleTimeout(Task task) {
        if (timeoutScheduler.isShutdown() || timeoutScheduler.isTerminated()) {
            return;
        }

        Runnable timeoutHandler = () -> checkTaskTimeout(task);

        timeoutFutures.put(
                task,
                timeoutScheduler.schedule(
                        timeoutHandler,
                        WORKER_TIMEOUT_MILLISECONDS,
                        TimeUnit.MILLISECONDS)
        );
    }

    private void checkTaskTimeout(Task task) {
        if (task.isAssigned() && task.isExpired(WORKER_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS)) {
            timeoutFutures.remove(task);
            if (mapResults.containsKey(task)) {
                return;
            }
            logger.info("Map task {} returned to queue due to timeout", task.getId());
            if (!taskQueue.offer(task)) {
                logger.error("Cannot return timed out task {} to queue", task.getId());
            }
            task.setAssigned(false);
        }
    }

    private void finish() {
        working = false;
        timeoutScheduler.shutdown();
    }

    private void clearQueues() {
        taskQueue.clear();
        cancelScheduledTimeoutChecks();
    }

    private void cancelScheduledTimeoutChecks() {
        timeoutFutures.values().forEach(future -> future.cancel(false));
        timeoutFutures.clear();
    }

    private void waitForMapTasksCompletion() {
        try {
            mapLatch.await();
        } catch (InterruptedException e) {
            interrupt();
            logger.error("Map tasks interrupted");
        }
    }

    private void waitForReduceTasksCompletion() {
        try {
            reduceLatch.await();
        } catch (InterruptedException e) {
            interrupt();
            logger.error("Reduce tasks interrupted");
        }
    }

    private void writeResult() {
        List<String> sortedResult = reduceResults.values().stream()
                .flatMap(res -> res.keyValueSet().stream())
                .sorted(Comparator.comparingInt((KeyValue kv) -> Integer.parseInt(kv.value())).reversed()
                        .thenComparing(KeyValue::key))
                .map(KeyValue::toString)
                .toList();

        try {
            Files.write(Paths.get("%s/result.txt".formatted(workDir)), sortedResult, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            logger.error("Cannot write result to file: {}", e.getMessage());
        }
    }
}
