package edu.auth.jetproud.proud.source.streams;

import edu.auth.jetproud.model.AnyProudData;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class ListStreamGenerator implements StreamGenerator
{
    private boolean usesRealTime;
    private LinkedList<AnyProudData> data;

    private long readStartTimeMillis = -1;
    private long streamTime = -1;

    public ListStreamGenerator(List<AnyProudData> data, boolean usesRealTime) {
        this.data = new LinkedList<>(data);
        this.usesRealTime = usesRealTime;
    }

    @Override
    public boolean isComplete() {
        return data.isEmpty();
    }

    @Override
    public List<AnyProudData> availableItems() {
        if (readStartTimeMillis < 0)
            readStartTimeMillis = System.currentTimeMillis();

        if (streamTime == -1) {
            streamTime = usesRealTime ? System.currentTimeMillis() : 0;
        } else {
            long delta = System.currentTimeMillis() - readStartTimeMillis;
            streamTime += delta;
        }

        List<AnyProudData> matches = data.stream()
                .filter((it)->it.arrival <= streamTime)
                .collect(Collectors.toList());

        data.removeAll(matches);
        return matches;
    }

    @Override
    public void close() {
        data.clear();
    }
}
