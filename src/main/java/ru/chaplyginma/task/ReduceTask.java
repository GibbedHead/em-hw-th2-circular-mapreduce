package ru.chaplyginma.task;

import java.util.Collection;

public class ReduceTask extends Task {

    private final int id;
    private final Collection<MapTaskResult> results;

    public ReduceTask(int id, Collection<MapTaskResult> results) {
        this.id = id;
        this.results = results;
    }

    public int getId() {
        return id;
    }
}
