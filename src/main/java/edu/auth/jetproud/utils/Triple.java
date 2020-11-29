package edu.auth.jetproud.utils;

import java.io.Serializable;
import java.util.Objects;

public class Triple<First, Second, Third> implements Serializable
{

    public First first;
    public Second second;
    public Third third;

    public Triple() {
        this(null, null, null);
    }

    public Triple(First first, Second second, Third third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;

        if (!Objects.equals(first, triple.first)) return false;
        if (!Objects.equals(second, triple.second)) return false;
        return Objects.equals(third, triple.third);
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        result = 31 * result + (third != null ? third.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                "1=" + first +
                ", 2=" + second +
                ", 3=" + third +
                '}';
    }
}
