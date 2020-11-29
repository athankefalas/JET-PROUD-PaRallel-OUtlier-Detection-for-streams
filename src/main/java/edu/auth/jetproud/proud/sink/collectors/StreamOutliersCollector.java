package edu.auth.jetproud.proud.sink.collectors;

import edu.auth.jetproud.model.meta.OutlierQuery;

import java.io.Serializable;

public interface StreamOutliersCollector extends Serializable
{
    void collect(long windowKey, OutlierQuery query, long outlierCount);

    void close();
}
