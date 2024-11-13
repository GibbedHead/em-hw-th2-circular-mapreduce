package ru.chaplyginma.manager;

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

    private static final int WORKER_TIMEOUT_MILLISECONDS = 100;

    private final Set<String> files;
    private final int numReduceTasks;
    private final String workDir;
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final Map<Task, ScheduledFuture<?>> timeoutFutures = new ConcurrentHashMap<>();
    private final CountDownLatch mapLatch;
    private final CountDownLatch reduceLatch;
    private final Lock lock = new ReentrantLock();
    private final Map<Task, MapTaskResult> mapResults = new ConcurrentHashMap<>();
    private final Map<Task, ReduceTaskResult> reduceResults = new ConcurrentHashMap<>();
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
        System.out.println("Manager: Started");

        createMapTasks();
        waitForMapTasksCompletion();
        System.out.println("Manager: Map tasks completed");

        clearQueues();

        createReduceTasks();
        waitForReduceTasksCompletion();
        System.out.println("Manager: Reduce tasks completed");

        writeResult();

        clearQueues();

        finish();
        System.out.println("Manager finished.");
    }

    public Task getTask(Thread thread) throws InterruptedException {
        final Task task = taskQueue.poll(1, TimeUnit.MILLISECONDS);
        if (task != null) {
            if (isTaskCompleted(task)) {
                System.out.printf("%s: Taking completed task: %s%n", thread.getName(), task.getId());
                return null;
            }
            task.start();
            scheduleTimeout(task);
        }
        return task;
    }

    public void completeMapTask(MapTask task, MapTaskResult mapTaskResult, Thread thread) {
        completeTask(task, mapTaskResult, thread, mapResults, mapLatch, "map");
    }

    public void completeReduceTask(ReduceTask task, ReduceTaskResult result, Thread thread) {
        completeTask(task, result, thread, reduceResults, reduceLatch, "reduce");
    }

    public boolean isWorking() {
        return working;
    }

    private boolean isTaskCompleted(Task task) {
        return mapResults.containsKey(task) || reduceResults.containsKey(task);
    }

    private <T> void completeTask(Task task, T taskResult, Thread thread,
                                  Map<Task, T> resultMap,
                                  CountDownLatch latch,
                                  String taskType) {
        lock.lock();
        try {
            if (resultMap.containsKey(task)) {
                System.out.printf("%s: Result dropped for task %d %n", thread.getName(), task.getId());
                return;
            }

            ScheduledFuture<?> scheduledFuture = timeoutFutures.remove(task);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }

            resultMap.put(task, taskResult);
            latch.countDown();
            System.out.printf("%s: Completed %s task %d %n", thread.getName(), taskType, task.getId());
        } finally {
            lock.unlock();
        }
    }

    private void createMapTasks() {
        for (String file : files) {
            taskQueue.add(new MapTask(file, numReduceTasks));
        }
        System.out.printf("Manager: Map tasks created%n");
    }

    private void createReduceTasks() {
        Task.resetGenerator();
        for (int i = 0; i < numReduceTasks; i++) {
            taskQueue.add(new ReduceTask(getMapFilesByReduceId(i)));
        }
        System.out.printf("Manager: Reduce tasks created%n");
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
            System.out.println("Scheduler is shutting down, task " + task.getId() + " cannot be scheduled.");
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
        lock.lock();
        try {
            if (task.isAssigned() && task.isExpired(WORKER_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS)) {
                timeoutFutures.remove(task);
                if (mapResults.containsKey(task)) {
                    return;
                }
                System.out.println("Map task " + task.getId() + " returned to queue due to timeout");
                if (!taskQueue.offer(task)) {
                    System.out.printf("Cannot return timed out task %s to queue%n", task.getId());
                }
                task.setAssigned(false);
            }
        } finally {
            lock.unlock();
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
            System.out.println("Map tasks wait interrupted");
        }
    }

    private void waitForReduceTasksCompletion() {
        try {
            reduceLatch.await();
        } catch (InterruptedException e) {
            interrupt();
            System.out.println("Reduce tasks wait interrupted");
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
            System.out.printf("Cannot write result to file: %s%n", e.getMessage());
        }
    }
}
