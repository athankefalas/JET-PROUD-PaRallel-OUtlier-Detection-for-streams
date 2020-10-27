package edu.auth.jetproud.proud.distributables;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;

import java.io.Serializable;
import java.util.Set;

public abstract class AnyDistributedObject<O extends DistributedObject> implements Serializable
{

    private final String name;
    private transient HazelcastInstance hazelcastInstance;

    public AnyDistributedObject(String name) {
        this.name = name;
    }

    protected final HazelcastInstance hazelcastInstance() {
        // Get the currently active or create a new HazelcastInstance
        // in this cluster node in order to get the underlying structure
        // this work around is required since HazelcastInstance class
        // and all structure classes (PNCounter, IMap, ReplicatedMap)
        // are not Serializable.
        if (hazelcastInstance == null) {
            Set<HazelcastInstance> all = Hazelcast.getAllHazelcastInstances();

            if (all.isEmpty()) // Force create a new hazelcast instance
                hazelcastInstance = Jet.bootstrappedInstance().getHazelcastInstance();
            else // Get currently active hazelcast instance - should always go here
                hazelcastInstance = all.stream().findFirst().orElse(null);
        }

        return hazelcastInstance;
    }

    // Getters

    public String getName() {
        return name;
    }


    // Requirements

    protected final O getData() {
        return getData(hazelcastInstance(), name);
    }

    protected abstract O getData(HazelcastInstance hazelcastInstance, String name);

}
