package ru.chaplyginma.task;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class Task {

    private Instant startTime;
    private boolean isAssigned;


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
}
