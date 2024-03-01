package io.stolther.soundcheck.temp;

import io.stolther.soundcheck.core.DataSeries;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import java.util.ArrayList;
import java.util.Random;

public class ProfilingExample {


    private static final double VALUE_UNCERTAINTY_N_SIGMA = 1.0; // Replace with your actual value
    private static final Random rand = new Random();

    public static void main(String[] args) {
        int iterations = 100000;
        int arraySize = 100;
        double[] x = new double[arraySize];
        double[] y = new double[arraySize];

        // Fill arrays with some random data
        for (int i = 0; i < arraySize; i++) {
            x[i] = Math.random();
            y[i] = Math.random();
        }

        // Profile Apache Commons Math PearsonsCorrelation
        long startTime1 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            PearsonsCorrelation correlation = new PearsonsCorrelation();
            double result = correlation.correlation(x, y);
        }
        long endTime1 = System.nanoTime();
        long duration1 = (endTime1 - startTime1);
        System.out.println("Time taken with Apache Commons Math PearsonsCorrelation: " + duration1 + " ns");

        // Profile custom Pearson's correlation method
        long startTime2 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            double result = pearsonCorrelation(x, y);
        }
        long endTime2 = System.nanoTime();
        long duration2 = (endTime2 - startTime2);
        System.out.println("Time taken with custom Pearson's correlation method: " + duration2 + " ns");
    }

    public static double pearsonCorrelation(double[] x, double[] y) {
        int n = x.length;
        double sumX = 0.0, sumY = 0.0, sumXY = 0.0, sumX2 = 0.0, sumY2 = 0.0;

        for (int i = 0; i < n; i++) {
            sumX += x[i];
            sumY += y[i];
            sumXY += x[i] * y[i];
            sumX2 += x[i] * x[i];
            sumY2 += y[i] * y[i];
        }

        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

        if (denominator == 0) {
            return 0; // or throw an exception
        }

        return numerator / denominator;
    }

    public static void profilenormaldist(String[] args) {
        int iterations = 100000;
        double mean = 0.0;
        double stdDev = 1.0;

        // Profile Apache Commons Math NormalDistribution
        long startTime1 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            NormalDistribution dist = new NormalDistribution(mean, stdDev * VALUE_UNCERTAINTY_N_SIGMA);
            double sample = dist.sample();
        }
        long endTime1 = System.nanoTime();
        long duration1 = (endTime1 - startTime1);
        System.out.println("Time taken with Apache Commons Math NormalDistribution: " + duration1 + " ns");

        // Profile custom Box-Muller method
        long startTime2 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            double sample = boxMullerSample(mean, stdDev * VALUE_UNCERTAINTY_N_SIGMA);
        }
        long endTime2 = System.nanoTime();
        long duration2 = (endTime2 - startTime2);
        System.out.println("Time taken with custom Box-Muller method: " + duration2 + " ns");
    }

    private static double boxMullerSample(double mean, double stdDev) {
        double u1 = rand.nextDouble();
        double u2 = rand.nextDouble();
        double z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        return z0 * stdDev + mean;
    }

    public static void profileArrayListreuse(String[] args) {
        int iterations = 100000; // Number of iterations to simulate window operations
        int listSize = 10; // Size of the list to simulate the window size
        Random rand = new Random();

        // Profile with new ArrayList creation each time
        long startTime1 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            ArrayList<Double> list = new ArrayList<>();
            for (int j = 0; j < listSize; j++) {
                list.add(rand.nextDouble());
            }
        }
        long endTime1 = System.nanoTime();
        long duration1 = (endTime1 - startTime1);
        System.out.println("Time taken with new ArrayList each time: " + duration1 + " ns");

        // Profile with ArrayList reuse
        ArrayList<Double> reusableList = new ArrayList<>();
        long startTime2 = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            reusableList.clear();
            for (int j = 0; j < listSize; j++) {
                reusableList.add(rand.nextDouble());
            }
        }
        long endTime2 = System.nanoTime();
        long duration2 = (endTime2 - startTime2);
        System.out.println("Time taken with ArrayList reuse: " + duration2 + " ns");
    }


    public static void profilestd(String[] args) {
        // Generate some sample data
        double[] x = new double[1000];
        for (int i = 0; i < x.length; i++) {
            x[i] = Math.random();
        }

        // Profile Apache Commons Math StandardDeviation
        long startTime1 = System.nanoTime();
        StandardDeviation sd = new StandardDeviation();
        double std1 = sd.evaluate(x);
        long endTime1 = System.nanoTime();
        long duration1 = (endTime1 - startTime1);
        System.out.println("Time taken for Apache Commons Math StandardDeviation: " + duration1 + " ns, Result: " + std1);

        // Profile custom StandardDeviation
        long startTime2 = System.nanoTime();
        double std2 = calculateStandardDeviation(x);
        long endTime2 = System.nanoTime();
        long duration2 = (endTime2 - startTime2);
        System.out.println("Time taken for custom StandardDeviation: " + duration2 + " ns, Result: " + std2);
    }

    public static double calculateStandardDeviation(double[] x) {
        int n = x.length;
        double sum = 0;
        for (double v : x) {
            sum += v;
        }
        double mean = sum / n;
        double sumOfSquares = 0;
        for (double v : x) {
            sumOfSquares += Math.pow(v - mean, 2);
        }
        return Math.sqrt(sumOfSquares / n);
    }
}
