package edu.auth.jetproud.utils;

import java.io.Serializable;
import java.util.function.Supplier;

public class Lazy<T extends Serializable>
{

    private T value;
    private Supplier<T> supplier;

    public Lazy(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T value() {
        if (value == null) {
            value = supplier.get();
        }

        return value;
    }
}
