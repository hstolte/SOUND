package io.stolther.soundcheck.core;

public class VA2CheckResult {

    public long timestamp;

    Outcome outcome;
    int alpha;
    int beta;
    int n_samples_tot; // 0 if no sampling was required to determine outcome


    public final double[] values_0;
    public final double[] values_1;
    public long nPoints_0;
    public long nPoints_1;

    public double meanUncertainty_0;
    public double meanUncertainty_1;

    public double dataMean_0;
    public double dataMean_1;

    public String key;

    public VA2CheckResult(long timestamp, Outcome outcome, int alpha, int beta, int n_samples_tot, String key, long nPoints_0, double meanUncertainty_0, double dataMean_0, long nPoints_1, double meanUncertainty_1, double dataMean_1, double[] values_0, double[] values_1) {
        this.outcome = outcome;
        this.alpha = alpha;
        this.beta = beta;
        this.n_samples_tot = n_samples_tot;
        this.timestamp = timestamp;
        this.key = key;
        this.nPoints_0 = nPoints_0;
        this.nPoints_1 = nPoints_1;
        this.meanUncertainty_0 = meanUncertainty_0;
        this.meanUncertainty_1 = meanUncertainty_1;
        this.dataMean_0 = dataMean_0;
        this.dataMean_1 = dataMean_1;
        this.values_0 = values_0;
        this.values_1 = values_1;
    }

    public static String get_csv_header() {
        return "timestamp,outcome,alpha,beta,n_samples_tot,key,n_points_0,meanuncertainty_0,valuemean_0,n_points_1,meanuncertainty_1,valuemean_1";
    }

    @Override
    public String toString() {
        return "CheckResult{" +
                "timestamp=" + timestamp +
                ", outcome=" + outcome.getValue() +
                ", alpha=" + alpha +
                ", beta=" + beta +
                ", n_samples_tot=" + n_samples_tot +
                ", key=" + key +
                ", nPoints_0=" + nPoints_0 +
                ", nPoints_1=" + nPoints_1 +
                ", meanUncertainty_0=" + meanUncertainty_0 +
                ", meanUncertainty_1=" + meanUncertainty_1 +
                ", dataMean_0=" + dataMean_0 +
                ", dataMean_1=" + dataMean_1 +
                '}';
    }

    public String to_csv() {
        return timestamp + "," + outcome.getValue() + "," + alpha + "," + beta + "," + n_samples_tot + "," + key + "," + nPoints_0 + "," + meanUncertainty_0 + "," + dataMean_0 + "," + nPoints_1 + "," + meanUncertainty_1 + "," + dataMean_1;
    }

    public double[] get_values(int id) {
        switch (id) {
            case 0:
                return values_0;
            case 1:
                return values_1;
            case 2:
                return new double[]{timestamp};
            default:
                throw new IllegalArgumentException("unkown windowvalue id");
        }
    }
}
