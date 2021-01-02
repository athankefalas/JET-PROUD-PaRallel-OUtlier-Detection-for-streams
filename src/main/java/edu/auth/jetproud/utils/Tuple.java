package edu.auth.jetproud.utils;

import java.io.Serializable;
import java.util.Map;

public class Tuple<E1, E2> implements Map.Entry<E1, E2>, Serializable
{
    public E1 first;
    public E2 second;

    public Tuple() {
        this(null, null);
    }

    public Tuple(E1 first, E2 second) {
        this.first = first;
        this.second = second;
    }

    public E1 getFirst() {
        return first;
    }

    public void setFirst(E1 first) {
        this.first = first;
    }

    public E2 getSecond() {
        return second;
    }

    public void setSecond(E2 second) {
        this.second = second;
    }

    public Tuple<E1,E2> copy() {
        return new Tuple<>(first, second);
    }

    // Entry Impl

    public static <K,V> Tuple<K,V> fromEntry(Map.Entry<K,V> entry) {
        return new Tuple<>(entry.getKey(), entry.getValue());
    }

    @Override
    public E1 getKey() {
        return first;
    }

    @Override
    public E2 getValue() {
        return second;
    }

    @Override
    public E2 setValue(E2 value) {
        second = value;
        return second;
    }

    @Override
    public String toString() {
        return "{" +
                "1=" + first +
                ", 2=" + second +
                '}';
    }
}
