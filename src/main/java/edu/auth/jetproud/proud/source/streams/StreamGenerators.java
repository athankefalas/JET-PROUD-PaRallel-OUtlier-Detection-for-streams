package edu.auth.jetproud.proud.source.streams;

import edu.auth.jetproud.model.AnyProudData;

import java.util.List;

public final class StreamGenerators
{

    public static StreamGenerator realTimeItemsIn(List<AnyProudData> data) {
        return new ListStreamGenerator(data, true);
    }

    public static StreamGenerator simulatedTimeItemsIn(List<AnyProudData> data) {
        return new ListStreamGenerator(data, true);
    }

}
