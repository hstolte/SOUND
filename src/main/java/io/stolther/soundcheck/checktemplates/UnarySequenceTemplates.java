package io.stolther.soundcheck.checktemplates;

/**
 * Container class for different types of unary sequence constraint checks.
 */
public class UnarySequenceTemplates {

    /**
     * Checks if the sequence is strictly increasing.
     */
    public static boolean monotonicIncrease(double[] x) {
        for (int i = 1; i < x.length; i++) {
            if (x[i] < x[i - 1]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if the sequence is monotonically decreasing.
     */
    public static boolean monotonicDecrease(double[] x) {
        for (int i = 1; i < x.length; i++) {
            if (x[i] > x[i - 1]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if the sequence is strictly monotonically increasing.
     */
    public static boolean strictMonotonicIncrease(double[] x) {
        for (int i = 1; i < x.length; i++) {
            if (x[i] <= x[i - 1]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if the sequence is strictly monotonically decreasing.
     */
    public static boolean strictMonotonicDecrease(double[] x) {
        for (int i = 1; i < x.length; i++) {
            if (x[i] >= x[i - 1]) {
                return false;
            }
        }
        return true;
    }


    /**
     * Checks for a change in trend (e.g., from increasing to decreasing) within the sequence.
     */
    public static boolean trendChange(double[] x) {
        boolean increasing = x[1] > x[0];
        for (int i = 1; i < x.length - 1; i++) {
            boolean newIncreasing = x[i + 1] > x[i];
            if (newIncreasing != increasing) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the sequence oscillates around a specific value within a certain range.
     *
     * @param targetValue The value around which the sequence should oscillate.
     * @param range       The allowable range for oscillation.
     */
    public static boolean oscillationCheck(double[] x, final double targetValue, final double range) {
        boolean above = x[0] > targetValue;
        for (int i = 1; i < x.length; i++) {
            boolean newAbove = x[i] > targetValue;
            if (newAbove != above && Math.abs(x[i] - targetValue) <= range) {
                return true;
            }
        }
        return false;
    }

    /**
     * Counts the number of times the series changes sign.
     *
     * @param minTimes Minimum number of times the series should change sign.
     */
    public static boolean signChanges(double[] x, final int minTimes) {
        boolean positive = x[0] > 0;
        int signChanges = 0;
        for (int i = 1; i < x.length; i++) {
            boolean newPositive = x[i] > 0;
            if (newPositive != positive) {
                signChanges++;
            }
            positive = newPositive;
        }
        return signChanges >= minTimes;
    }
}
