package io.stolther.soundcheck.core;

import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public abstract class BinarySetCheck extends Check implements MapFunction<Tuple2<DataSeries, DataSeries>, CheckResult> {

    @Override
    public CheckResult map(Tuple2<DataSeries, DataSeries> dataSeriesDataSeriesTuple2) throws Exception {
        DataSeries s1 = dataSeriesDataSeriesTuple2.f0;
        DataSeries s2 = dataSeriesDataSeriesTuple2.f1;

        // we always have to do bootstrapping, to account for uncertainty from sparsity
        // even if there is no value uncertainty

        int n_satisfied = 0;
        int n_violated = 0;
        int n_samples_seen = 1;
        int outcome = 0;


        for (; n_samples_seen < N_SAMPLES + 1; n_samples_seen++) {

            double[] bootstrapSample1 = getBootstrapSample(s1);
            double[] bootstrapSample2 = getBootstrapSample(s2);


            boolean result = constraint(bootstrapSample1, bootstrapSample2);
            n_satisfied += result ? 1 : 0;
            n_violated = n_samples_seen - n_satisfied;
            outcome = decisionLookupTable[1 + n_satisfied][1 + n_violated];
            if (outcome > 0) {
                break;
            }
        }

        // todo remove
        assert s1.t == s2.t;

        return new CheckResult(s1.t,
                Outcome.fromValue(outcome),
                n_satisfied,
                n_violated,
                n_samples_seen,
                s1.key + "_" + s2.key);
    }


    public abstract boolean constraint(double[] x, double[] y);

    public static void main(String[] args) throws Exception {
        BinarySetCheck check = new BinarySetCheck() {
            @Override
            public boolean constraint(double[] x, double[] y) {
                return StatUtils.min(x) > StatUtils.max(y);
            }
        };

        System.out.println(check.map(new Tuple2<>(
                new DataSeries(1, new double[]{5, 8, 5}),
                new DataSeries(1, new double[]{2, 2, 4})
        )));
        System.out.println(check.map(new Tuple2<>(
                new DataSeries(1, new double[]{2, 2, 4}),
                new DataSeries(1, new double[]{5, 8, 5})
        )));

    }
}

