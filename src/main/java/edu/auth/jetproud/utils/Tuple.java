package edu.auth.jetproud.utils;

public class Tuple<E1, E2>
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
}
