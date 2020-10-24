package edu.auth.jetproud.proud.statistics;

import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.jet.JetInstance;

public class ProudStatistics
{
    // Singleton

    private static ProudStatistics _global;

    public static void createGlobalFor(JetInstance instance) {
        if (_global == null) {
            _global = new ProudStatistics(instance);
        }
    }

    public static ProudStatistics global() {
        return _global;
    }

    // Impl

    private final PNCounter slideCounter;
    private final PNCounter cpuTimeCounter;

    private ProudStatistics(JetInstance instance) {
        this.slideCounter = instance.getHazelcastInstance().getPNCounter("slideCounter");
        this.cpuTimeCounter = instance.getHazelcastInstance().getPNCounter("cpuTimeCounter");
    }

    public PNCounter getSlideCounter() {
        return slideCounter;
    }

    public PNCounter getCpuTimeCounter() {
        return cpuTimeCounter;
    }
}
