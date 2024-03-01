package io.stolther.soundcheck.core;

public class DataPoint {

    /**
     * timestamp
     */
    public final long t;

    /**
     * value
     */
    public final double v;


    /**
     * point uncertainty
     */
    public final double sigma;

    public final String key;


    public DataPoint(long t, double v, double sigma, String key) {
        this.t = t;
        this.v = v;
        this.sigma = sigma;
        this.key = key;
    }

    public DataPoint(long t, double v, double sigma) {
        this.t = t;
        this.v = v;
        this.sigma = sigma;
        this.key = null;
    }

    public DataPoint(long t, double v) {
        this.t = t;
        this.v = v;
        this.sigma = 0.0;
        this.key = null;
    }
}
