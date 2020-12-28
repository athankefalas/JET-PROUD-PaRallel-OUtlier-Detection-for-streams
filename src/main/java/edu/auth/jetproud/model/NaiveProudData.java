package edu.auth.jetproud.model;

import edu.auth.jetproud.utils.Lists;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NaiveProudData extends AnyProudData
{
    //Neighbor data
    public AtomicInteger count_after;
    public CopyOnWriteArrayList<Long> nn_before;
    // Skip flag
    public AtomicBoolean safe_inlier;

    public NaiveProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public NaiveProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        count_after = new AtomicInteger(0);
        nn_before = new CopyOnWriteArrayList<>();
        safe_inlier = new AtomicBoolean(false);
    }

    // Implementation

    public void insert_nn_before(long el) {
        insert_nn_before(el, 0);
    }

    //Function to insert data as a preceding neighbor (max k neighbors)
    public void insert_nn_before(long el, int k) {
        if (k != 0 && nn_before.size() == k) {
            long min = nn_before.stream()
                    .min(Long::compareTo)
                    .orElse(0L);

            if (el > min) {
                nn_before.removeIf((it)->it == min);
                nn_before.add(el);
            }

        } else {
            nn_before.add(el);
        }
    }

    //Get the minimum of preceding neighbors
    public Long get_min_nn_before(long time) {
        if (nn_before.stream().noneMatch(it -> it >= time))
            return 0L;
        else
            return nn_before.stream()
                .filter((it)->it>=time)
                .min(Long::compareTo)
                .orElse(0L);
    }

    // Clear Variables
    public void clear(int newMc) {
        nn_before.clear();
        count_after.set(0);
    }



}
