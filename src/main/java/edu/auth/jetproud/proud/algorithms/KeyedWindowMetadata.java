package edu.auth.jetproud.proud.algorithms;

import java.io.Serializable;

public class KeyedWindowMetadata implements Serializable
{
    public int partition;

    public long start;
    public long end;

    public KeyedWindowMetadata(int partition, long start, long end) {
        this.partition = partition;
        this.start = start;
        this.end = end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyedWindowMetadata that = (KeyedWindowMetadata) o;

        if (partition != that.partition) return false;
        if (start != that.start) return false;
        return end == that.end;
    }

    @Override
    public int hashCode() {
        int result = partition;
        result = 31 * result + (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }

}
