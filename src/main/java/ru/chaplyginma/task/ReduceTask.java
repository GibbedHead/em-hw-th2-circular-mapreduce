package ru.chaplyginma.task;

import java.util.Set;

/**
 * Represents a reducing task that operates on a set of input files.
 *
 * <p>The {@code ReduceTask} class extends the {@link Task} class and is designed
 * to manage the reduction phase of data processing. It takes a set of files that
 * contain the intermediate results from mapping tasks and processes them to produce
 * the final output.</p>
 */
public class ReduceTask extends Task {

    private final Set<String> files;

    /**
     * Constructs a new ReduceTask with the specified set of input files.
     *
     * @param files the set of input files associated with this reduce task.
     */
    public ReduceTask(Set<String> files) {
        this.files = files;
    }

    /**
     * Gets the set of input files that will be processed by this reduce task.
     *
     * @return the set of input files.
     */
    public Set<String> getFiles() {
        return files;
    }
}
