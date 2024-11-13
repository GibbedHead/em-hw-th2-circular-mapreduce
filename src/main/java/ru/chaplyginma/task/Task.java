package ru.chaplyginma.task;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class Task {

    private static int idGenerator = 0;

    private final int id;
    private Instant startTime;
    private volatile boolean isAssigned;

    public Task() {
        this.id = getNextId();
    }

    public void start() {
        this.startTime = Instant.now();
        this.isAssigned = true;
    }

    public boolean isExpired(long timeout, TimeUnit unit) {
        Instant expirationTime = startTime.plusNanos(unit.toNanos(timeout));
        return Instant.now().isAfter(expirationTime);
    }

    public boolean isAssigned() {
        return isAssigned;
    }

    public void setAssigned(boolean assigned) {
        isAssigned = assigned;
    }

    public int getId() {
        return id;
    }

    private static int getNextId() {
        return idGenerator++;
    }

    public static void resetGenerator() {
        idGenerator = 0;
    }
}
