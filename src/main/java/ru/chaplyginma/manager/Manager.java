package ru.chaplyginma.manager;

import ru.chaplyginma.task.MapTask;
import ru.chaplyginma.task.MapTaskResult;

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

    private final BlockingQueue<MapTask> mapTaskQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Map<MapTask, ScheduledFuture<?>> mapTaskScheduledFutures = new ConcurrentHashMap<>();
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
        finish();
    }

    public MapTask getNextMapTask(Thread thread) throws InterruptedException {
        lock.lock();
        System.out.println(thread.getName() + ": getNextMapTask");
        try {
            if (mapTaskScheduledFutures.isEmpty() && mapTaskQueue.isEmpty()) {
                return null;
            }

            while (!mapTaskScheduledFutures.isEmpty() && mapTaskQueue.isEmpty()) {
                System.out.println(thread.getName() + " waiting for map tasks...");
                rescheduledCondition.await();
                System.out.println(thread.getName() + " rescheduled.");
            }

            final MapTask task = mapTaskQueue.poll(1, TimeUnit.MILLISECONDS);
            if (task == null) {
                return null;
            }
            System.out.println("task " + task.getId() + " taken. remaining tasks");
            System.out.println(mapTaskQueue);
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
            ScheduledFuture<?> scheduledFuture = mapTaskScheduledFutures.remove(task);
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
        int taskId = 1;
        for (String file : files) {
            MapTask mapTask = new MapTask(
                    taskId++,
                    file,
                    numReduceTasks
            );
            mapTaskQueue.add(mapTask);
        }
    }

    private void scheduleMapTimeout(MapTask task) {
        if (scheduler.isShutdown() || scheduler.isTerminated()) {
            System.out.println("Scheduler is shutting down, task " + task.getId() + " cannot be scheduled.");
            return; // Do not schedule the timeout
        }

        Runnable timeoutHandler = () -> {
            lock.lock();
            try {
                // Проверяем, истек ли таймаут выполнения
                if (task.isAssigned() && task.isExpired(WORKER_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS)) {
                    mapTaskScheduledFutures.remove(task);
                    if (resultMap.containsKey(task)) {
                        return;
                    }
                    mapTaskQueue.offer(task); // Возвращаем задачу в очередь
                    System.out.println("Map task " + task.getId() + " returned to queue due to timeout");
                    System.out.println(mapTaskQueue);
                    // Удаляем таймер
                    rescheduledCondition.signal();
                    task.setAssigned(false);
                    System.out.println("Task " + task.getId() + " timed out");
                }
            } finally {
                lock.unlock();
            }
        };

        mapTaskScheduledFutures.put(
                task,
                scheduler.schedule(
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
        System.out.println(mapTaskQueue);
        System.out.println(mapTaskScheduledFutures);
        System.out.println("Clearing map queue");
        mapTaskQueue.clear();
        scheduler.shutdown();
        lock.lock();
        try {
            rescheduledCondition.signalAll();
        } finally {
            lock.unlock();
        }
        System.out.println("Manager finished.");
    }

}
