package ru.chaplyginma.manager;

import ru.chaplyginma.task.MapTask;

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
        System.out.println(mapLatch.getCount());
        System.out.println("Manager finished.");
    }

    public MapTask getNextMapTask(Thread thread) throws InterruptedException {
        lock.lock();
        try {
            if (mapTaskScheduledFutures.isEmpty() && mapTaskQueue.isEmpty()) {
                return null;
            }

            while (!mapTaskScheduledFutures.isEmpty() && mapTaskQueue.isEmpty()) {
                System.out.println(thread.getName() +  ": Waiting for map tasks to be scheduled");
                rescheduledCondition.await();
                System.out.println(thread.getName() +  ": Rescheduled");
            }

            final MapTask task = mapTaskQueue.poll(1, TimeUnit.MILLISECONDS);
            if (task == null) {
                return null;
            }
            task.start();
            scheduleMapTimeout(task);
            return task;
        } finally {
            lock.unlock();
        }
    }

    public void completeMapTask(MapTask task) {
        lock.lock();
        try {
            ScheduledFuture<?> scheduledFuture = mapTaskScheduledFutures.remove(task);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false); // Отменяем таймер выполнения
            }
            mapLatch.countDown();
            System.out.println("Map task " + task + " completed.");
            rescheduledCondition.signalAll();
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
        Runnable timeoutHandler = () -> {
            lock.lock();
            try {
                // Проверяем, истек ли таймаут выполнения
                if (task.isAssigned() && task.isExpired(WORKER_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS)) {
                    mapTaskQueue.offer(task); // Возвращаем задачу в очередь
                    mapTaskScheduledFutures.remove(task); // Удаляем таймер
                    rescheduledCondition.signalAll();
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

}