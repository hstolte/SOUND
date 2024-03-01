package io.stolther.soundcheck.core;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public abstract class VABinarySequenceCheck extends Check implements MapFunction<Tuple2<DataSeries, DataSeries>, VA2CheckResult> {
    @Override
    public VA2CheckResult map(Tuple2<DataSeries, DataSeries> dataSeriesDataSeriesTuple2) throws Exception {
        DataSeries s1 = dataSeriesDataSeriesTuple2.f0;
        DataSeries s2 = dataSeriesDataSeriesTuple2.f1;

        int n_satisfied = 0;
        int n_violated = 0;
        int n_samples_seen = 1;
        int outcome = 0;

        int n_observations = s1.v.length;
        int block_length = (int) Math.floor(Math.sqrt(n_observations));
        int numBlocks = (int) Math.floor((double) n_observations / block_length);


        for (; n_samples_seen < N_SAMPLES + 1; n_samples_seen++) {
            double[] bootstrapSample1 = new double[n_observations];
            double[] bootstrapSample2 = new double[n_observations];
            int sampleIndex = 0;

            for (int i_block = 0; i_block < numBlocks; i_block++) {
                int randomBlockStart = ThreadLocalRandom.current().nextInt(n_observations - block_length + 1);

                for (int i = 0; i < block_length; i++) {
                    int currentIndex = randomBlockStart + i;

                    bootstrapSample1[sampleIndex] = sampleValue(s1, currentIndex);
                    bootstrapSample2[sampleIndex] = sampleValue(s2, currentIndex);

                    sampleIndex++;
                }
            }

            boolean result = constraint(bootstrapSample1, bootstrapSample2);
            n_satisfied += result ? 1 : 0;
            n_violated = n_samples_seen - n_satisfied;
            outcome = decisionLookupTable[1 + n_satisfied][1 + n_violated];
            if (outcome > 0) {
                break;
            }
        }

        VA2CheckResult result = new VA2CheckResult(
                Math.max(s1.t, s2.t),
                Outcome.fromValue(outcome),
                n_violated,
                n_satisfied,
                n_samples_seen,
                s1.key + s2.key,
                n_observations,
                (s1.sigma != null) ? MathUtils.mean(s1.sigma) : 0.0,
                MathUtils.mean(s1.v),
                n_observations,
                (s2.sigma != null) ? MathUtils.mean(s2.sigma) : 0.0,
                MathUtils.mean(s2.v),
                s1.v,
                s2.v
        );
        return result;

    }

    // Helper function to sample a value based on the DataSeries and index
    private double sampleValue(DataSeries series, int index) {
        if (series.sigma != null) {
            return MathUtils.normalSample(series.v[index], series.sigma[index] * VALUE_UNCERTAINTY_N_SIGMA);
        }
        return series.v[index];
    }

    public abstract boolean constraint(double[] x, double[] y);
}
