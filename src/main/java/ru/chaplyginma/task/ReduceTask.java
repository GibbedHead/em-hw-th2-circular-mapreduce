package ru.chaplyginma.task;

import java.util.Set;

public class ReduceTask extends Task {

    private final Set<String> files;

    public ReduceTask(Set<String> files) {
        this.files = files;
    }

    public Set<String> getFiles() {
        return files;
    }
}
