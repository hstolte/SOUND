package io.stolther.soundcheck.core;

import org.apache.flink.api.common.functions.MapFunction;


public abstract class BinaryPointBasedCheck extends Check implements MapFunction<DataPoint2, CheckResult> {
    protected abstract boolean constraint(double x, double y);

    @Override
    public CheckResult map(DataPoint2 p) throws Exception {
        if (p.sigma1 == 0.0 && p.sigma2 == 0.0) {
            return new CheckResult(p.t,
                    constraint(p.v1, p.v2) ? Outcome.SATISFIED : Outcome.VIOLATED,
                    0,
                    0,
                    0,
                    p.key);
        }

        int n_satisfied = 0;
        int n_violated = 0;
        int n_samples_seen = 1;
        int outcome = 0;

        for (; n_samples_seen < N_SAMPLES + 1; n_samples_seen++) {

            // sample from value uncertainties
            final double sample1 = MathUtils.normalSample(p.v1, p.sigma1 * VALUE_UNCERTAINTY_N_SIGMA);
            final double sample2 = MathUtils.normalSample(p.v2, p.sigma2 * VALUE_UNCERTAINTY_N_SIGMA);

            boolean result = constraint(sample1, sample2);
            n_satisfied += result ? 1 : 0;
            n_violated = n_samples_seen - n_satisfied;
            outcome = decisionLookupTable[1 + n_satisfied][1 + n_violated];
            if (outcome > 0) {
                break;
            }
        }

        return new CheckResult(p.t,
                Outcome.fromValue(outcome),
                n_satisfied,
                n_violated,
                n_samples_seen,
                p.key);
    }
}

