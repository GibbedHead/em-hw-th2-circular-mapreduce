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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class Manager extends Thread {

    private static final int WORKER_TIMEOUT_MILLISECONDS = 100;

    private final Set<String> files;
    private final int numReduceTasks;
    private final String workDir;

    private volatile boolean working = true;
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final Map<Task, ScheduledFuture<?>> taskScheduledFutures = new ConcurrentHashMap<>();

    private final CountDownLatch mapLatch;
    private final CountDownLatch reduceLatch;
    private final Lock lock = new ReentrantLock();
    private final Condition rescheduledCondition = lock.newCondition();

    private final Map<Task, MapTaskResult> mapTasksResultMap = new ConcurrentHashMap<>();
    private final Map<Task, ReduceTaskResult> reduceTasksResultMap = new ConcurrentHashMap<>();

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

        System.out.println("************************");
        printMapRes();
        printMapFilesTotalLines();
        System.out.println("************************");

        System.out.println("Manager: Map tasks completed");

        clearQueues();

        createReduceTasks();
        waitForReduceTasksCompletion();

        writeResult();

        working = false;
        clearQueues();

        System.out.println("Manager: Reduce tasks completed");

        finish();
    }

    private void printMapRes() {
        List<String> mapFiles =  mapTasksResultMap.values().stream()
                .flatMap(r -> r.files().stream())
                .toList();
        System.out.println("Map files number: " + mapFiles.size());
        //mapFiles.forEach(System.out::println);
    }

    private void printMapFilesTotalLines() {
        List<String> mapFiles =  mapTasksResultMap.values().stream()
                .flatMap(r -> r.files().stream())
                .toList();
        int count = 0;
        for (String mapFile : mapFiles) {
            try {
                count += Files.lines(Paths.get(mapFile)).count();
            } catch (IOException e) {
                System.out.println("oops");
            }
        }
        System.out.println("Map files total: " + count);
    }

    public Task getTask(Thread thread) throws InterruptedException {
        final Task task = taskQueue.poll(1, TimeUnit.MILLISECONDS);
        if (task != null) {
            if (mapTasksResultMap.containsKey(task) || reduceTasksResultMap.containsKey(task)) {
                System.out.printf("%s: Taking completed task: %s%n", thread.getName(), task.getId());
                return null;
            }
            task.start();
            scheduleTimeout(task);
        }
        return task;
    }

    public void completeMapTask(MapTask task, MapTaskResult mapTaskResult, Thread thread) {
        completeTask(task, mapTaskResult, thread, mapTasksResultMap, mapLatch, "map");
    }

    public void completeReduceTask(ReduceTask task, ReduceTaskResult result, Thread thread) {
        completeTask(task, result, thread, reduceTasksResultMap, reduceLatch, "reduce");
    }

    private <T> void completeTask(Task task, T taskResult, Thread thread,
                                  Map<Task, T> resultMap,
                                  CountDownLatch latch,
                                  String taskType) {
        lock.lock();
        try {
            if (resultMap.containsKey(task)) {
                rescheduledCondition.signalAll();
                System.out.printf("%s: Result dropped for task %d %n", thread.getName(), task.getId());
                return;
            }

            ScheduledFuture<?> scheduledFuture = taskScheduledFutures.remove(task);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }

            resultMap.put(task, taskResult);
            latch.countDown();
            rescheduledCondition.signal();
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
        return mapTasksResultMap.values().stream()
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

        taskScheduledFutures.put(
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
                taskScheduledFutures.remove(task);
                if (mapTasksResultMap.containsKey(task)) {
                    return;
                }
                System.out.println("Map task " + task.getId() + " returned to queue due to timeout");
                taskQueue.offer(task);
                rescheduledCondition.signal();
                task.setAssigned(false);
            }
        } finally {
            lock.unlock();
        }
    }

    private void finish() {
        timeoutScheduler.shutdown();
        System.out.println("Manager finished.");
    }

    private void clearQueues() {
        taskQueue.clear();
        cancelScheduledTimeoutChecks();

        lock.lock();
        try {
            rescheduledCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void cancelScheduledTimeoutChecks() {
        taskScheduledFutures.values().forEach(future -> future.cancel(false));
        taskScheduledFutures.clear();
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

    public boolean isWorking() {
        return working;
    }

    private void  writeResult() {
        List<KeyValue> ff = reduceTasksResultMap.values().stream()
                .flatMap(res -> res.keyValueSet().stream())
                .toList();
        List<KeyValue> f = ff.stream().filter(kv -> kv.key().equals("sam")).toList();

        List<String> d = reduceTasksResultMap.values().stream()
                .flatMap(res -> res.keyValueSet().stream())
                .sorted(Comparator.comparingInt((KeyValue kv) -> Integer.parseInt(kv.value())).reversed()
                        .thenComparing(KeyValue::key))
                .map(KeyValue::toString)
                .toList();
        String ffg = d.stream().filter(kv -> kv.contains("sam")).findFirst().orElse(null);

        try {
            Files.write(Paths.get("%s/result.txt".formatted(workDir)), d, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            System.out.printf("Cannot write result to file: %s%n", e.getMessage());
        }
    }
}
