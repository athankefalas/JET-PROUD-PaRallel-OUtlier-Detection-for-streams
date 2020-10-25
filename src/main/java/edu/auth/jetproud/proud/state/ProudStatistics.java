package edu.auth.jetproud.proud.state;

import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.jet.JetInstance;

public final class ProudStatistics
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


    public static String WINDOW_SLIDE_COUNTER = "WINDOW_SLIDE_COUNTER";
    public static String CPU_TIME_COUNTER = "CPU_TIME_COUNTER";

    // Impl
    private final JetInstance jetInstance;

    private ProudStatistics(JetInstance jetInstance) {
        this.jetInstance = jetInstance;
    }

    public PNCounter getSlideCounter() {
        return jetInstance.getHazelcastInstance().getPNCounter(WINDOW_SLIDE_COUNTER);
    }

    public PNCounter getCpuTimeCounter() {
        return jetInstance.getHazelcastInstance().getPNCounter(CPU_TIME_COUNTER);
    }
}
