package io.stolther.soundcheck.core;

import org.apache.flink.api.common.functions.MapFunction;


public abstract class UnaryPointBasedCheck extends Check implements MapFunction<DataPoint, CheckResult> {
    @Override
    public CheckResult map(DataPoint x) throws Exception {
        if (x.sigma == 0.0 || N_SAMPLES == 0) {
            return new CheckResult(x.t,
                    constraint(x.v) ? Outcome.SATISFIED : Outcome.VIOLATED,
                    0,
                    0,
                    0,
                    x.key);
        }

        int n_satisfied = 0;
        int n_violated = 0;
        int n_samples_seen = 1;
        int outcome = 0;

        for (; n_samples_seen < N_SAMPLES + 1; n_samples_seen++) {

            // sample from value uncertainties
            final double sample = MathUtils.normalSample(x.v, x.sigma * VALUE_UNCERTAINTY_N_SIGMA);

            boolean result = constraint(sample);
            n_satisfied += result ? 1 : 0;
            n_violated = n_samples_seen - n_satisfied;
            outcome = decisionLookupTable[1 + n_satisfied][1 + n_violated];
            if (outcome > 0) {
                break;
            }
        }

        return new CheckResult(x.t,
                Outcome.fromValue(outcome),
                n_satisfied,
                n_violated,
                n_samples_seen,
                x.key);
    }


    protected abstract boolean constraint(double x);

    public static void main(String[] args) throws Exception {
        UnaryPointBasedCheck check = new UnaryPointBasedCheck() {
            @Override
            public boolean constraint(double x) {
                return x > 0;
            }
        };

        System.out.println(check.map(new DataPoint(1, 1)));
        System.out.println(check.map(new DataPoint(1, 1, 1)));
        System.out.println(check.map(new DataPoint(1, 1, 1)));

    }
}

