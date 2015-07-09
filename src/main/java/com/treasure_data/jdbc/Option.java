package com.treasure_data.jdbc;

/**
 * Optional value representation
 */
public final class Option<T>
{
    private static final Option<?> EMPTY = new Option(null);

    private final T value;

    private Option(T value)
    {
        this.value = value;
    }

    public boolean isDefined() { return value != null; }

    public boolean isEmpty() { return value == null; }

    public T get() { return value; }

    public T getOrElse(T defaultValue)
    {
        return isEmpty() ? defaultValue : get();
    }

    public static <T> Option<T> of(T obj)
    {
        if (obj == null) {
            return (Option<T>) EMPTY;
        }
        else {
            return new Option(obj);
        }
    }

    public static <T> Option<T> empty()
    {
        return (Option<T>) EMPTY;
    }
}
