package edu.auth.jetproud.proud.state;

import com.hazelcast.collection.IList;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

public class ProudState
{
    // Singleton

    private static ProudState _global;

    public static void createGlobalFor(JetInstance instance) {
        if (_global == null) {
            _global = new ProudState(instance);
        }
    }

    public static ProudState global() {
        return _global;
    }

    private final JetInstance jetInstance;

    private ProudState(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
    }

    public JetInstance getJetInstance() {
        return jetInstance;
    }

    public HazelcastInstance getHazelcastInstance() {
        return jetInstance.getHazelcastInstance();
    }

    public <K, V> IMap<K, V> getMap(String name) {
        return jetInstance.getMap(name);
    }

    public <K, V> ReplicatedMap<K, V> getReplicatedMap(String name) {
        return jetInstance.getReplicatedMap(name);
    }

    public <E> IList<E> getList(String name) {
        return jetInstance.getList(name);
    }
}
