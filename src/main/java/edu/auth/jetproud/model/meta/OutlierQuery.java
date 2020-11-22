package edu.auth.jetproud.model.meta;

import java.io.Serializable;

public class OutlierQuery implements Serializable
{
    public double range;
    public int kNeighbours;

    public int window;
    public int slide;

    public long outlierCount;

    public OutlierQuery(double range, int kNeighbours, int window, int slide) {
        this(range, kNeighbours, window, slide, 0);
    }

    private OutlierQuery(double range, int kNeighbours, int window, int slide, long outlierCount) {
        this.range = range;
        this.kNeighbours = kNeighbours;
        this.window = window;
        this.slide = slide;
        this.outlierCount = outlierCount;
    }

    public OutlierQuery withOutlierCount(long outlierCount) {
        return new OutlierQuery(range, kNeighbours, window, slide, this.outlierCount);
    }

    public double getRange() {
        return range;
    }

    public int getKNeighbours() {
        return kNeighbours;
    }

    public int getWindow() {
        return window;
    }

    public int getSlide() {
        return slide;
    }

    public long getOutlierCount() {
        return outlierCount;
    }

    public boolean isInSameSpace(OutlierQuery other) {
        return kNeighbours == other.kNeighbours && range == other.range && window == other.window && slide == other.slide;
    }
}
