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

    private static final int WORKER_TIMEOUT_MILLISECONDS = 200;

    private final Set<String> files;
    private final int numReduceTasks;

    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1);
    private final Map<Task, ScheduledFuture<?>> taskScheduledFutures = new ConcurrentHashMap<>();

    private final CountDownLatch mapLatch;
    private final CountDownLatch reduceLatch;
    private final Lock lock = new ReentrantLock();
    private final Condition rescheduledCondition = lock.newCondition();

    private final Map<Task, MapTaskResult> resultMap = new ConcurrentSkipListMap<>(Comparator.comparingInt(Task::getId));

    public Manager(Set<String> files, int numReduceTasks) {
        this.files = files;
        this.mapLatch = new CountDownLatch(files.size());
        this.numReduceTasks = numReduceTasks;
        this.reduceLatch = new CountDownLatch(numReduceTasks);

    }

    @Override
    public void run() {
        createMapTasks();
        waitForMapTasksCompletion();
        clearQueues();

//        createReduceTasks();
//        waitForReduceTasksCompletion();

        finish();
    }

    public Task getNextMapTask(Thread thread) throws InterruptedException {
        lock.lock();
        try {
            if (taskScheduledFutures.isEmpty() && taskQueue.isEmpty()) {
                return null;
            }

            while (!taskScheduledFutures.isEmpty() && taskQueue.isEmpty()) {
                rescheduledCondition.await();
            }

            final Task task = taskQueue.poll(1, TimeUnit.MILLISECONDS);
            if (task != null) {
                task.start();
                scheduleTimeout(task);
            }
            return task;
        } finally {
            lock.unlock();
        }
    }

    public void completeMapTask(MapTask task, MapTaskResult mapTaskResult, Thread thread) {
        lock.lock();
        try {
            if (resultMap.containsKey(task)) {
                rescheduledCondition.signalAll();
                return;
            }
            ScheduledFuture<?> scheduledFuture = taskScheduledFutures.remove(task);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
            resultMap.put(task, mapTaskResult);
            mapLatch.countDown();
            rescheduledCondition.signal();
            System.out.printf("Map task %d completed by %s.%n", task.getId(), thread.getName());
        } finally {
            lock.unlock();
        }
    }

    public void completeReduceTask(ReduceTask task) {
        lock.lock();
        try {
            System.out.println("Reduce task id=" + task.getId() + " completed");
            reduceLatch.countDown();
        } finally {
            lock.unlock();
        }
    }

    private void createMapTasks() {
        for (String file : files) {
            taskQueue.add(new MapTask(file, numReduceTasks));
        }
    }

    private void createReduceTasks() {
        for (int i = 0; i < numReduceTasks; i++) {
            taskQueue.add(new ReduceTask(i, resultMap.values()));
        }
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
                if (resultMap.containsKey(task)) {
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
        System.out.println("-------------------------------");
        System.out.println("Results number: " + resultMap.size());
        System.out.println(resultMap);
        System.out.println("-------------------------------");
        timeoutScheduler.shutdown();
        System.out.println("Manager finished.");
    }

    private void clearQueues() {
        System.out.println(taskQueue);
        System.out.println(taskScheduledFutures);
        System.out.println("Clearing map queue");
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

}
