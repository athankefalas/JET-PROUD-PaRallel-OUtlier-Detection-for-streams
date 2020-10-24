package edu.auth.jetproud.proud.algorithms;

public class OutlierQuery
{
    public double r; // TODO refactor -> range
    public int k; // TODO refactor -> kNeighbours

    public int w; // TODO refactor -> window
    public int s; // TODO refactor s -> slide
    public int outliers; // TODO refactor outliers -> outlierCount

    public OutlierQuery(double r, int k, int w, int s) {
        this.r = r;
        this.k = k;
        this.w = w;
        this.s = s;
    }
}
