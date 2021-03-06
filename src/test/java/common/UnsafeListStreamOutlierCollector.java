package common;

import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.proud.sink.collectors.StreamOutliersCollector;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Triple;

import java.util.LinkedList;
import java.util.List;

public class UnsafeListStreamOutlierCollector implements StreamOutliersCollector
{

    public boolean debugReporting = true;

    @Override
    public void collect(long windowKey, OutlierQuery query, long outlierCount) {

        if (debugReporting) {
            String queryString = "(r="+query.range+", k="+query.kNeighbours+", w="+query.window+", s="+query.slide+")";
            System.out.println("DEBUG: Slide("+windowKey+"), Query"+queryString+". Outliers: "+outlierCount+".");
        }

        DistributedMap<String, List<Triple<Long, OutlierQuery, Long>>> dItems = new DistributedMap<>("OUT_MAP");

        List<Triple<Long, OutlierQuery, Long>> items = dItems.getOrDefault("ITEMS", Lists.make());
        items.add(new Triple<>(windowKey, query, outlierCount));

        dItems.put("ITEMS", items);
    }

    public List<Triple<Long, OutlierQuery, Long>> items() {
        DistributedMap<String, List<Triple<Long, OutlierQuery, Long>>> dItems = new DistributedMap<>("OUT_MAP");
        return dItems.getOrDefault("ITEMS", Lists.make());
    }

    @Override
    public void close() {

    }
}
