package ru.chaplyginma.task;

/**
 * Represents a mapping task associated with a specific input file and the number of reduce tasks.
 *
 * <p>The {@code MapTask} class extends the {@link Task} class and is designed to handle
 * tasks that involve processing data from a specified input file. It also maintains information
 * about the number of reduce tasks that need to be performed after the mapping operation.</p>
 */
public class MapTask extends Task {

    private final String file;
    private final int numReduceTasks;

    /**
     * Constructs a new MapTask with the specified input file and number of reduce tasks.
     *
     * @param file           the name of the input file associated with this map task.
     * @param numReduceTasks the number of reduce tasks to be executed after the mapping process.
     */
    public MapTask(String file, int numReduceTasks) {
        this.file = file;
        this.numReduceTasks = numReduceTasks;
    }

    /**
     * Gets the name of the input file associated with this map task.
     *
     * @return the name of the input file.
     */
    public String getFile() {
        return file;
    }

    /**
     * Gets the number of reduce tasks that will be executed after the mapping process.
     *
     * @return the number of reduce tasks.
     */
    public int getNumReduceTasks() {
        return numReduceTasks;
    }
}
