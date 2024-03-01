package io.stolther.soundcheck.core;

import io.palyvos.provenance.util.ExperimentSettings;

import java.io.Serializable;

public abstract class Check implements Serializable {
    public int N_SAMPLES = -1;
    public double VALUE_UNCERTAINTY_N_SIGMA;
    public int[][] decisionLookupTable = null;

    public Check init(ExperimentSettings settings) {
        assert settings.getCI() > 0;
        
        N_SAMPLES = settings.getNSamples();
        VALUE_UNCERTAINTY_N_SIGMA = settings.getValueUncertaintyNSigma();
        decisionLookupTable = EvalLookUpTable.getOrCreateLookupTable(N_SAMPLES, settings.getCI(), 0.5);
        return this;
    }

//    public void init(int N_SAMPLES, double VALUE_UNCERTAINTY_N_SIGMA) {
//        this.N_SAMPLES = N_SAMPLES;
//        this.VALUE_UNCERTAINTY_N_SIGMA = VALUE_UNCERTAINTY_N_SIGMA;
//    }

    public double[] getBootstrapSample(DataSeries s) {
        double[] bootstrapSample = new double[s.v.length];

        for (int i = 0; i < s.v.length; i++) {
            int randomIndex = (int) (Math.random() * s.v.length);

            if (s.sigma != null) {
                // if value uncertainty is present, sample from it
                bootstrapSample[i] = MathUtils.normalSample(s.v[randomIndex], s.sigma[randomIndex] * VALUE_UNCERTAINTY_N_SIGMA);
            } else {
                // otherwise just use the nominal value
                bootstrapSample[i] = s.v[randomIndex];
            }
        }
        return bootstrapSample;
    }
}