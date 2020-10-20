package edu.auth.jetproud.proud.partitioning;

import edu.auth.jetproud.model.AnyProudData;

public class PartitionedData<Data>
{
    private int partition;
    private Data data;

    public PartitionedData() {
        this(-1, null);
    }

    public PartitionedData(int partition, Data data) {
        this.partition = partition;
        this.data = data;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }
}
