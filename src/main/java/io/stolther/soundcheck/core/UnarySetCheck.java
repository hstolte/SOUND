package io.stolther.soundcheck.core;

import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.flink.api.common.functions.MapFunction;


public abstract class UnarySetCheck extends Check implements MapFunction<DataSeries, CheckResult> {
    @Override
    public CheckResult map(DataSeries s) throws Exception {
        // we always have to do bootstrapping, to account for uncertainty from sparsity
        // even if there is no value uncertainty

        int n_satisfied = 0;
        int n_violated = 0;
        int n_samples_seen = 1;
        int outcome = 0;


        for (; n_samples_seen < N_SAMPLES + 1; n_samples_seen++) {

            double[] bootstrapSample = getBootstrapSample(s);

            boolean result = constraint(bootstrapSample);
            n_satisfied += result ? 1 : 0;
            n_violated = n_samples_seen - n_satisfied;
            outcome = decisionLookupTable[1 + n_satisfied][1 + n_violated];
            if (outcome > 0) {
                break;
            }
        }

        return new CheckResult(s.t,
                Outcome.fromValue(outcome),
                n_satisfied,
                n_violated,
                n_samples_seen,
                s.key);
    }


    public abstract boolean constraint(double[] x);


    public static void main(String[] args) throws Exception {
        UnarySetCheck check = new UnarySetCheck() {
            @Override
            public boolean constraint(double[] x) {
                return StatUtils.mean(x) > 2;
            }
        };

        System.out.println(check.map(new DataSeries(1, new double[]{2, 2, 3})));
        System.out.println(check.map(new DataSeries(1, new double[]{2, 2, 2}, new double[]{1, 1, 1})));

    }
}

