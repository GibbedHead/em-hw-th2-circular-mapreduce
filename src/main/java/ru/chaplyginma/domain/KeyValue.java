package ru.chaplyginma.domain;

public record KeyValue(
        String key,
        String value
) {
    @Override
    public String toString() {
        return "%s\t%s".formatted(key, value);
    }
}
