package io.stolther.soundcheck.core;

public class DataPoint2 {

    /**
     * timestamp
     */
    public final long t;

    /**
     * value 1
     */
    public final double v1;

    /**
     * value 2
     */
    public final double v2;

    /**
     * point uncertainty 1
     */
    public final double sigma1;

    /**
     * point uncertainty 2
     */
    public final double sigma2;

    public final String key;

    public DataPoint2(long t, double v1, double v2, double sigma1, double sigma2) {
        this.t = t;
        this.v1 = v1;
        this.v2 = v2;
        this.sigma1 = sigma1;
        this.sigma2 = sigma2;
        this.key = null;
    }

    public DataPoint2(long t, double v1, double v2) {
        this.t = t;
        this.v1 = v1;
        this.v2 = v2;
        this.sigma1 = 0;
        this.sigma2 = 0;
        this.key = null;
    }
}

