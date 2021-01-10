package edu.auth.jetproud.proud.partitioning.custom;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.Lists;

import java.util.List;

public final class PartitioningState
{
    private ProudContext proudContext;
    protected List<PartitionedData<AnyProudData>> data;

    public PartitioningState(ProudContext proudContext) {
        this.proudContext = proudContext;
        data = Lists.make();
    }

    private AnyProudData flagPoint(AnyProudData point, int flag) {
        return new AnyProudData(point.id, point.value, point.arrival, flag);
    }

    public void pointIncludedIn(AnyProudData point, int partition) {
        data.add(new PartitionedData<>(partition, flagPoint(point, 0)));
    }

    public void pointExcludedFrom(AnyProudData point, int partition) {
        data.add(new PartitionedData<>(partition, flagPoint(point, 1)));
    }

    protected List<PartitionedData<AnyProudData>> getData() {
        assert data.size() == proudContext.internalConfiguration().getPartitions();
        return Lists.copyOf(data);
    }

}
