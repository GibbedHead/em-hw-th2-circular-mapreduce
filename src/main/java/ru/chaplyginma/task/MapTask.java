package ru.chaplyginma.task;

public class MapTask extends Task {

    private final String file;
    private final int numReduceTasks;

    public MapTask(String file, int numReduceTasks) {
        this.file = file;
        this.numReduceTasks = numReduceTasks;
    }

    public String getFile() {
        return file;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }
}
