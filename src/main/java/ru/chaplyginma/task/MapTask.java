package ru.chaplyginma.task;

public class MapTask extends Task {

    private final int id;
    private final String file;
    private final int numReduceTasks;

    public MapTask(int id, String file, int numReduceTasks) {
        this.id = id;
        this.file = file;
        this.numReduceTasks = numReduceTasks;
    }

    public int getId() {
        return id;
    }

    public String getFile() {
        return file;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }

    @Override
    public String toString() {
        return "MapTask{" +
                "id=" + id +
                '}';
    }
}
