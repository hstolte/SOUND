package io.stolther.soundcheck.core;

import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.flink.api.common.functions.MapFunction;

public abstract class UnarySequenceCheck extends Check implements MapFunction<DataSeries, CheckResult> {

    @Override
    public CheckResult map(DataSeries s) throws Exception {
        int n_satisfied = 0;
        int n_violated = 0;
        int n_samples_seen = 1;
        int outcome = 0;

        int n_observations = s.v.length;
        int block_length = (int) Math.floor(Math.sqrt(n_observations));
        int numBlocks = (int) Math.floor((double) n_observations / block_length);

        for (; n_samples_seen < N_SAMPLES + 1; n_samples_seen++) {

            double[] bootstrapSample = new double[n_observations];
            int sampleIndex = 0;

            for (int i_block = 0; i_block < numBlocks; i_block++) {
                int randomBlockStart = (int) (Math.random() * (n_observations - block_length + 1));

                for (int i = 0; i < block_length; i++) {
                    int currentIndex = randomBlockStart + i;

                    if (s.sigma != null) {
                        // if value uncertainty is present, sample from it
                        bootstrapSample[sampleIndex] = MathUtils.normalSample(s.v[currentIndex], s.sigma[currentIndex] * VALUE_UNCERTAINTY_N_SIGMA);
                    } else {
                        // otherwise just use the nominal value
                        bootstrapSample[sampleIndex] = s.v[currentIndex];
                    }

                    sampleIndex++;
                }
            }

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
        UnarySequenceCheck check = new UnarySequenceCheck() {
            @Override
            public boolean constraint(double[] x) {
                return StatUtils.mean(x) > 2;
            }
        };

        System.out.println(check.map(new DataSeries(1, new double[]{2, 2, 3})));
        System.out.println(check.map(new DataSeries(1, new double[]{2, 2, 2}, new double[]{1, 1, 1})));

    }
}

