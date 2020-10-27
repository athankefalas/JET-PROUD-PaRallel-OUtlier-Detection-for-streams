package edu.auth.jetproud.model.meta;

public class OutlierQuery
{
    public double r; // TODO refactor -> range
    public int k; // TODO refactor -> kNeighbours

    public int w; // TODO refactor -> window
    public int s; // TODO refactor s -> slide
    public int outliers; // TODO refactor outliers -> outlierCount

    public OutlierQuery(double r, int k, int w, int s) {
        this(r, k, w, s, 0);
    }

    private OutlierQuery(double r, int k, int w, int s, int outliers) {
        this.r = r;
        this.k = k;
        this.w = w;
        this.s = s;
        this.outliers = outliers;
    }

    public OutlierQuery withOutlierCount(int outlierCount) {
        return new OutlierQuery(r, k, w, s, outliers);
    }
}
