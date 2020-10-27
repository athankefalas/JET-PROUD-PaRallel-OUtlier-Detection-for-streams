package edu.auth.jetproud.proud.distributables;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.PNCounter;

import java.io.Serializable;

public class DistributedCounter extends AnyDistributedObject<PNCounter> implements PNCounter, Serializable
{
    public DistributedCounter(String name) {
        super(name);
    }

    @Override
    protected PNCounter getData(HazelcastInstance hazelcastInstance, String name) {
        return hazelcastInstance.getPNCounter(name);
    }

    // Proxy Impl

    @Override
    public long get() {
        return getData().get();
    }

    @Override
    public long getAndAdd(long delta) {
        return getData().getAndAdd(delta);
    }

    @Override
    public long addAndGet(long delta) {
        return getData().addAndGet(delta);
    }

    @Override
    public long getAndSubtract(long delta) {
        return getData().getAndSubtract(delta);
    }

    @Override
    public long subtractAndGet(long delta) {
        return getData().subtractAndGet(delta);
    }

    @Override
    public long decrementAndGet() {
        return getData().decrementAndGet();
    }

    @Override
    public long incrementAndGet() {
        return getData().incrementAndGet();
    }

    @Override
    public long getAndDecrement() {
        return getData().getAndDecrement();
    }

    @Override
    public long getAndIncrement() {
        return getData().getAndIncrement();
    }

    @Override
    public void reset() {
        getData().reset();
    }

    @Override
    public String getPartitionKey() {
        return getData().getPartitionKey();
    }

    @Override
    public String getName() {
        return getData().getName();
    }

    @Override
    public String getServiceName() {
        return getData().getServiceName();
    }

    @Override
    public void destroy() {
        getData().destroy();
    }
}
