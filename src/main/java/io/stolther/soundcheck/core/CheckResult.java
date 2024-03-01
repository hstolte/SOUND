package io.stolther.soundcheck.core;

public class CheckResult {

    public long timestamp;

    public double violationProb;

    Outcome outcome;
    int alpha;
    int beta;
    int n_samples_tot; // 0 if no sampling was required to determine outcome

    public String key;


    public CheckResult(long timestamp, Outcome outcome, int n_satisfied, int n_violated, int n_samples_tot, String key) {
        this.timestamp = timestamp;
        this.outcome = outcome;
        this.alpha = 1 + n_violated;
        this.beta = 1 + n_satisfied;
        this.n_samples_tot = n_samples_tot;
        this.key = key;
    }


    public static String get_csv_header() {
        return "timestamp,outcome,n_samples_tot,alpha,beta,key";
    }

    @Override
    public String toString() {
        return "CheckResult{" +
                "timestamp=" + timestamp +
                ", outcome=" + outcome.getValue() +
                ", n_samples_tot=" + n_samples_tot +
                ", alpha=" + alpha +
                ", beta=" + beta +
                ", key=" + key +
                '}';
    }

    public String to_csv() {
        return timestamp + "," + outcome.getValue() + "," + n_samples_tot + "," + alpha + "," + beta + "," + key;
    }
}
