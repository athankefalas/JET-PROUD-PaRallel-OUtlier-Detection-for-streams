package edu.auth.jetproud.proud.sink.collectors;

import edu.auth.jetproud.model.meta.OutlierQuery;

public interface StreamOutliersCollector
{
    void collect(long windowKey, OutlierQuery query, long outlierCount);

    void close();
}
