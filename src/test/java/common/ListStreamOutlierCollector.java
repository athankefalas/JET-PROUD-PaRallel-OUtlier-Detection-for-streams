package common;

import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.sink.collectors.StreamOutliersCollector;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Triple;

import java.util.LinkedList;

public class ListStreamOutlierCollector implements StreamOutliersCollector
{
    public LinkedList<Triple<Long, OutlierQuery, Long>> items = Lists.makeLinkedList();

    @Override
    public void collect(long windowKey, OutlierQuery query, long outlierCount) {
        items.add(new Triple<>(windowKey, query, outlierCount));
    }

    @Override
    public void close() {

    }
}
