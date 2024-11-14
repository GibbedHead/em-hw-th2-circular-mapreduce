package ru.chaplyginma.domain;

/**
 * A record that represents a key-value pair.
 *
 * <p>The {@code KeyValue} class encapsulates a single key and its associated value.
 * This class is immutable.</p>
 *
 * @param key   the key of the key-value pair, cannot be null
 * @param value the value associated with the key, cannot be null
 */
public record KeyValue(
        String key,
        String value
) {
    @Override
    public String toString() {
        return "%s\t%s".formatted(key, value);
    }
}
