package edu.auth.jetproud.proud.state;

import edu.auth.jetproud.proud.distributables.DistributedCounter;
import edu.auth.jetproud.utils.Lazy;


/**
 * ProudStatistics
 *
 * A static container for holding the slide count and CPU time distributed counters.
 * @Deprecated Use the built in Metrics API
 */
@Deprecated
public final class ProudStatistics
{

    public static String WINDOW_SLIDE_COUNTER = "WINDOW_SLIDE_COUNTER";
    public static String CPU_TIME_COUNTER = "CPU_TIME_COUNTER";


    private static Lazy<DistributedCounter> lazySlideCounter = new Lazy<>(()->new DistributedCounter(WINDOW_SLIDE_COUNTER));
    private static Lazy<DistributedCounter> lazyCPUTimeCounter = new Lazy<>(()->new DistributedCounter(CPU_TIME_COUNTER));

    public static DistributedCounter slideCounter() {
        return lazySlideCounter.value();
    }

    public static DistributedCounter cpuTimeCounter() {
        return lazyCPUTimeCounter.value();
    }
}
