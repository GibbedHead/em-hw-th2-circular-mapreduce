package ru.chaplyginma.manager;

import ru.chaplyginma.task.MapTask;
import ru.chaplyginma.task.MapTaskResult;
import ru.chaplyginma.task.ReduceTask;
import ru.chaplyginma.task.Task;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Manager extends Thread {

    private static final int WORKER_TIMEOUT_MILLISECONDS = 300;

    private final Set<String> files;
    private final int numReduceTasks;

    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final Map<Task, ScheduledFuture<?>> taskScheduledFutures = new ConcurrentHashMap<>();

    private final CountDownLatch mapLatch;
    private final Lock lock = new ReentrantLock();
    private final Condition rescheduledCondition = lock.newCondition();

    private final Map<MapTask, MapTaskResult> resultMap = new ConcurrentSkipListMap<>(Comparator.comparingInt(MapTask::getId));

    public Manager(Set<String> files, int numReduceTasks) {
        this.files = files;
        this.mapLatch = new CountDownLatch(files.size());
        this.numReduceTasks = numReduceTasks;

    }

    @Override
    public void run() {
        createMapTasks();
        try {
            mapLatch.await();
        } catch (InterruptedException e) {
            interrupt();
            System.out.println("Map tasks manager work interrupted. Exception: " + e.getMessage());
        }
        createReduceTasks();
        finish();
    }

    public Task getNextMapTask(Thread thread) throws InterruptedException {
        lock.lock();
        System.out.println(thread.getName() + ": getNextMapTask");
        try {
            if (taskScheduledFutures.isEmpty() && taskQueue.isEmpty()) {
                return null;
            }

            while (!taskScheduledFutures.isEmpty() && taskQueue.isEmpty()) {
                System.out.println(thread.getName() + " waiting for map tasks...");
                rescheduledCondition.await();
                System.out.println(thread.getName() + " rescheduled.");
            }

            final Task task = taskQueue.poll(1, TimeUnit.MILLISECONDS);
            if (task == null) {
                return null;
            }
            System.out.println("task " + task.getId() + " taken. remaining tasks");
            System.out.println(taskQueue);
            task.start();
            scheduleMapTimeout(task);
            return task;
        } finally {
            lock.unlock();
        }
    }

    public void completeMapTask(MapTask task, MapTaskResult mapTaskResult) {
        lock.lock();
        try {
            if (resultMap.containsKey(task)) {
                rescheduledCondition.signalAll();
                return;
            }
            ScheduledFuture<?> scheduledFuture = taskScheduledFutures.remove(task);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false); // Отменяем таймер выполнения
            }
            resultMap.put(task, mapTaskResult);
            mapLatch.countDown();
            rescheduledCondition.signal();
            System.out.println("Map task " + task + " completed.");

        } finally {
            lock.unlock();
        }
    }

    private void createMapTasks() {
        for (String file : files) {
            MapTask mapTask = new MapTask(
                    file,
                    numReduceTasks
            );
            taskQueue.add(mapTask);
        }
    }

    private void createReduceTasks() {
        for (int i = 0; i < numReduceTasks; i++) {
            ReduceTask reduceTask = new ReduceTask(i, resultMap.values());
            taskQueue.add(reduceTask);
        }
    }

    private void scheduleMapTimeout(Task task) {
        if (timeoutScheduler.isShutdown() || timeoutScheduler.isTerminated()) {
            System.out.println("Scheduler is shutting down, task " + task.getId() + " cannot be scheduled.");
            return; // Do not schedule the timeout
        }

        Runnable timeoutHandler = () -> {
            lock.lock();
            try {
                // Проверяем, истек ли таймаут выполнения
                if (task.isAssigned() && task.isExpired(WORKER_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS)) {
                    taskScheduledFutures.remove(task);
                    if (resultMap.containsKey(task)) {
                        return;
                    }
                    taskQueue.offer(task); // Возвращаем задачу в очередь
                    System.out.println("Map task " + task.getId() + " returned to queue due to timeout");
                    System.out.println(taskQueue);
                    // Удаляем таймер
                    rescheduledCondition.signal();
                    task.setAssigned(false);
                    System.out.println("Task " + task.getId() + " timed out");
                }
            } finally {
                lock.unlock();
            }
        };

        taskScheduledFutures.put(
                task,
                timeoutScheduler.schedule(
                        timeoutHandler,
                        WORKER_TIMEOUT_MILLISECONDS,
                        TimeUnit.MILLISECONDS)
        );
    }

    private void finish() {
        System.out.println("-------------------------------");
        System.out.println("Results number: " + resultMap.size());
        System.out.println(resultMap);
        System.out.println("-------------------------------");
        System.out.println(taskQueue);
        System.out.println(taskScheduledFutures);
        System.out.println("Clearing map queue");
        taskQueue.clear();
        timeoutScheduler.shutdown();
        lock.lock();
        try {
            rescheduledCondition.signalAll();
        } finally {
            lock.unlock();
        }
        System.out.println("Manager finished.");
    }

}
