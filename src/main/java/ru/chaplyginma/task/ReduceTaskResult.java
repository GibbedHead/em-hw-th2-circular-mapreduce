package ru.chaplyginma.task;

import ru.chaplyginma.domain.KeyValue;

import java.util.Set;

/**
 * Represents the result of a reducing task, containing a set of key-value pairs.
 *
 * <p>The {@code ReduceTaskResult} record encapsulates the results produced by a reduce task,
 * specifically a set of key-value pairs that represent the final output after processing
 * intermediate results from map tasks.</p>
 */
public record ReduceTaskResult(
        Set<KeyValue> keyValueSet
) {
}
