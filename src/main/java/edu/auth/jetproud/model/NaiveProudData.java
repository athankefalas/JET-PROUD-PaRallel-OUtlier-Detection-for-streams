package edu.auth.jetproud.model;

import edu.auth.jetproud.utils.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class NaiveProudData extends AnyProudData
{
    public UUID uuid = UUID.randomUUID();

    //Neighbor data
    public int count_after;
    public ArrayList<Long> nn_before;
    // Skip flag
    public boolean safe_inlier;

    public NaiveProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public NaiveProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        count_after = 0;
        nn_before = new ArrayList<>();
        safe_inlier = false;
    }

    // Copy @See resources/info/ReferenceIssues for issues related to memory alloc & management in java
    public NaiveProudData copy() {
        NaiveProudData data = new NaiveProudData(this);
        data.flag = flag;
        data.count_after = count_after;
        data.nn_before.addAll(nn_before);
        data.safe_inlier = safe_inlier;

        return data;
    }

    // Implementation

    public void insert_nn_before(long el) {
        insert_nn_before(el, 0);
    }

    //Function to insert data as a preceding neighbor (max k neighbors)
    public void insert_nn_before(long el, int k) {
        if (k != 0 && nn_before.size() == k) {
            Long min = nn_before.stream()
                    .min(Long::compareTo)
                    .orElse(null);

            assert min != null;

            if (el > min) {
                nn_before.remove(min);
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
        count_after = 0;
    }



}
