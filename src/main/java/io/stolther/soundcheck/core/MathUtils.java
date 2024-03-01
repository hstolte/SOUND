package io.stolther.soundcheck.core;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class MathUtils {

    /**
     * Perform element-wise division of two arrays of doubles.
     *
     * @param x the numerator array
     * @param y the denominator array
     * @return a new array containing the element-wise division results
     */
    public static double[] elementWiseDivision(double[] x, double[] y) {
        if (x.length != y.length) {
            throw new IllegalArgumentException("Arrays must have the same length");
        }

        int n = x.length;
        double[] result = new double[n];

        for (int i = 0; i < n; i++) {
            if (y[i] == 0) {
                throw new ArithmeticException("Division by zero at index " + i);
            }
            result[i] = x[i] / y[i];
        }

        return result;
    }

    /**
     * Calculate the mean of an array of doubles.
     *
     * @param x the input array
     * @return the mean
     */
    public static double mean(double[] x) {
        int n = x.length;
        if (n == 0) {
            throw new IllegalArgumentException("Array length cannot be 0");
        }

        double sum = 0;
        for (double v : x) {
            sum += v;
        }

        return sum / n;
    }

    /**
     * Calculate the standard deviation of an array of doubles.
     *
     * @param x the input array
     * @return the standard deviation
     */
    public static double std(double[] x) {
        int n = x.length;
        if (n == 0) {
            throw new IllegalArgumentException("Array length cannot be 0");
        }

        double sum = 0;
        for (double v : x) {
            sum += v;
        }

        double mean = sum / n;
        double sumOfSquares = 0;
        for (double v : x) {
            sumOfSquares += (v - mean) * (v - mean);
        }

        return Math.sqrt(sumOfSquares / n);
    }

    /**
     * Generate a normally distributed random number or an array of such numbers using the Box-Muller method.
     *
     * @param mean     the mean of the distribution
     * @param stdDev   the standard deviation of the distribution
     * @param nSamples the number of samples to generate (optional)
     * @return a sample or array of samples from the normal distribution
     */
    public static double[] normalSample(double mean, double stdDev, int nSamples) {
        double[] samples = new double[nSamples];
        for (int i = 0; i < nSamples; i++) {
            double u1 = ThreadLocalRandom.current().nextDouble();
            double u2 = ThreadLocalRandom.current().nextDouble();
            double z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
            samples[i] = z0 * stdDev + mean;
        }
        return samples;
    }

    /**
     * Generate a normally distributed random number using the Box-Muller method.
     *
     * @param mean   the mean of the distribution
     * @param stdDev the standard deviation of the distribution
     * @return a sample from the normal distribution
     */
    public static double normalSample(double mean, double stdDev) {
        double u1 = ThreadLocalRandom.current().nextDouble();
        double u2 = ThreadLocalRandom.current().nextDouble();
        double z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        return z0 * stdDev + mean;
    }

    /**
     * Calculate Pearson's correlation coefficient for two arrays.
     *
     * @param x the first array
     * @param y the second array
     * @return the Pearson's correlation coefficient
     * @throws IllegalArgumentException if the denominator is zero
     */
    public static double pearsonCorrelation(double[] x, double[] y) throws IllegalArgumentException {
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
            return Double.NaN;
//            throw new IllegalArgumentException("Denominator is zero, cannot calculate Pearson's correlation. \n" + Arrays.toString(x) + "\n" + Arrays.toString(y));
        }

        return numerator / denominator;
    }
}