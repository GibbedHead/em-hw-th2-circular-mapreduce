package ru.chaplyginma.task;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a task that can be assigned and tracked for expiration.
 *
 * <p>The {@code Task} class provides functionality to create and manage a task,
 * including automatic ID generation, assignment status, and expiration based on a timeout.</p>
 *
 * <p>Each task is identified by a unique ID, automatically assigned upon creation.
 * The task can be marked as assigned and started, recording the start time.</p>
 *
 * <p>Tasks can also be checked for expiration based on a specified timeout period,
 * ensuring that the task's lifecycle can be monitored effectively.</p>
 */
public class Task {

    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    private final int id;
    private Instant startTime;
    private volatile boolean isAssigned;

    /**
     * Constructs a new Task with a unique identifier.
     *
     * <p>The ID is assigned automatically using an atomic ID generator to ensure uniqueness.</p>
     */
    public Task() {
        this.id = getNextId();
    }

    /**
     * Generates the next unique task ID.
     *
     * @return the next unique task ID as an integer.
     */
    private static int getNextId() {
        return idGenerator.getAndIncrement();
    }

    /**
     * Resets the ID generator to start assigning IDs from zero again.
     *
     * <p>This method is typically used for testing or resetting purposes.</p>
     */
    public static void resetGenerator() {
        idGenerator.set(0);
    }

    /**
     * Marks the task as started and records the current time as the start time.
     *
     * <p>This method sets the assignment status to true and captures the moment
     * the task starts.</p>
     */
    public void start() {
        this.startTime = Instant.now();
        this.isAssigned = true;
    }

    /**
     * Checks if the task has expired based on the given timeout and time unit.
     *
     * @param timeout the amount of time to wait before considering the task expired.
     * @param unit    the unit of time for the timeout.
     * @return {@code true} if the task is expired; {@code false} otherwise.
     */
    public boolean isExpired(long timeout, TimeUnit unit) {
        Instant expirationTime = startTime.plusNanos(unit.toNanos(timeout));
        return Instant.now().isAfter(expirationTime);
    }

    /**
     * Checks if the task is currently assigned.
     *
     * @return {@code true} if the task is assigned; {@code false} otherwise.
     */
    public boolean isAssigned() {
        return isAssigned;
    }

    /**
     * Sets the assigned status of the task.
     *
     * @param assigned the new assigned status for the task.
     */
    public void setAssigned(boolean assigned) {
        isAssigned = assigned;
    }

    /**
     * Gets the unique identifier of the task.
     *
     * @return the unique ID of the task.
     */
    public int getId() {
        return id;
    }
}
