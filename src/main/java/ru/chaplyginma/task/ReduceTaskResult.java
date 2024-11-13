package ru.chaplyginma.task;

import ru.chaplyginma.domain.KeyValue;

import java.util.Set;

public record ReduceTaskResult(
        Set<KeyValue> keyValueSet
) {
}
