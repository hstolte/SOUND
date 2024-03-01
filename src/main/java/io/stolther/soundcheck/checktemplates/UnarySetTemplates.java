package io.stolther.soundcheck.checktemplates;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class UnarySetTemplates {

    /**
     * Checks if the array has no outliers larger than given threshold in Z-score from the median or mean.
     *
     * @param maxSigma Maximum number of standard deviations for a value to be considered an outlier.
     * @param fromMean If true, calculate from the mean; otherwise, from the median.
     */
    public static boolean hasNoOutliersAbove(double[] x, final double maxSigma, final boolean fromMean) {
        DescriptiveStatistics stats = new DescriptiveStatistics(x);
        double center = fromMean ? stats.getMean() : stats.getPercentile(50);
        double stdDev = stats.getStandardDeviation();

        for (double val : x) {
            if (Math.abs(val - center) > maxSigma * stdDev) {
                return false;
            }
        }

        return true;
    }

//    /**
//     * Checks if the values in the array fall within a given proportion of a quantile.
//     *
//     * @param quantile The quantile (e.g., 0.25 for 25th percentile).
//     * @param minProportion Minimum proportion of elements to check.
//     * @param maxProportion Maximum proportion of elements to check.
//     */
//    public static UnarySetCheck quantileCheck(final double quantile, final double minProportion, final double maxProportion) {
//        return new UnarySetCheck() {
//            @Override
//            public boolean constraint(double[] x) {
//                DescriptiveStatistics stats = new DescriptiveStatistics(x);
//                double threshold = stats.getPercentile(quantile * 100);
//                long count = Arrays.stream(x).filter(val -> val < threshold).count();
//                double proportion = (double) count / x.length;
//
//                return proportion >= minProportion && proportion <= maxProportion;
//            }
//        };
//    }

//    /**
//     * Checks if a specific category in the array falls within given percentage bounds.
//     *
//     * @param category The category value to check.
//     * @param minPercentage Minimum percentage for the category.
//     * @param maxPercentage Maximum percentage for the category.
//     */
//    public static UnarySetCheck categoryPercentage(final double category, final double minPercentage, final double maxPercentage) {
//        return new UnarySetCheck() {
//            @Override
//            public boolean constraint(double[] x) {
//                long count = Arrays.stream(x).filter(val -> val == category).count();
//                double proportion = (double) count / x.length;
//
//                return proportion >= minPercentage && proportion <= maxPercentage;
//            }
//        };
//    }
}
