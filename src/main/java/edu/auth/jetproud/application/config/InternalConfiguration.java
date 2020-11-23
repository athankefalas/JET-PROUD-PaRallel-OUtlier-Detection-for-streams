package edu.auth.jetproud.application.config;

import edu.auth.jetproud.utils.GCD;

import java.util.List;

public final class InternalConfiguration {

    private int partitions;

    private int commonW;
    private int commonS;
    private double commonR;

    private int allowedLateness;


    private InternalConfiguration() {

    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getCommonW() {
        return commonW;
    }

    public void setCommonW(int commonW) {
        this.commonW = commonW;
    }

    public int getCommonS() {
        return commonS;
    }

    public void setCommonS(int commonS) {
        this.commonS = commonS;
    }

    public double getCommonR() {
        return commonR;
    }

    public void setCommonR(double commonR) {
        this.commonR = commonR;
    }

    public int getAllowedLateness() {
        return allowedLateness;
    }

    public void setAllowedLateness(int allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    // Static init

    public static InternalConfiguration createFrom(ProudConfiguration proudConfiguration) {
        InternalConfiguration config = new InternalConfiguration();

        config.partitions = 16;

        List<Integer> w = proudConfiguration.getWindowSizes();
        config.commonW = w.size() > 1 ? w.stream().max(Integer::compareTo).orElse(0) : w.get(0);

        List<Integer> s = proudConfiguration.getSlideSizes();
        config.commonS = s.size() > 1 ? GCD.ofElementsIn(s).orElse(0) : s.get(0);

        List<Double> r = proudConfiguration.getRNeighbourhood();
        config.commonR = r.size() > 1 ? r.stream().max(Double::compareTo).orElse(0.0) : r.get(0);

        config.allowedLateness = config.commonS;

        return config;
    }

}
