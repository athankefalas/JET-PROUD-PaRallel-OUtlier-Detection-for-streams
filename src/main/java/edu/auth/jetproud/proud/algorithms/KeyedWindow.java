package edu.auth.jetproud.proud.algorithms;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.utils.Lists;

import java.io.Serializable;
import java.util.List;

public class KeyedWindow<T extends AnyProudData> implements Serializable
{
    public int partition;

    public long start;
    public long end;

    public List<T> data;

    public KeyedWindow(int partition, long start, long end) {
        this(partition, start, end, Lists.make());
    }

    public KeyedWindow(int partition, long start, long end, List<T> data) {
        this.partition = partition;
        this.start = start;
        this.end = end;
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyedWindow that = (KeyedWindow) o;

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
