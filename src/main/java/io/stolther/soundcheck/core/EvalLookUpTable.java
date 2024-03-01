package io.stolther.soundcheck.core;

import org.apache.commons.math3.distribution.BetaDistribution;

public class EvalLookUpTable {

    // Singleton instance
    private static int[][] singletonLookupTable = null;

    // Private constructor to prevent instantiation
    private EvalLookUpTable() {
    }

    public static int[][] getOrCreateLookupTable(int maxSampleSize, double credibilityLevel, double decisionThreshold) {
        // Create the lookup table only if it doesn't already exist
        if (singletonLookupTable == null) {
            singletonLookupTable = createLookupTable(maxSampleSize, credibilityLevel, decisionThreshold);
        }
        // Return the existing (or newly created) instance
        return singletonLookupTable;
    }


    public static int[][] createLookupTable(int maxSampleSize, double credibilityLevel, double decisionThreshold) {
        maxSampleSize += 1;

        int[][] lookupTable = new int[maxSampleSize + 1][maxSampleSize + 1];

        for (int alphaPost = 1; alphaPost <= maxSampleSize; alphaPost++) {
            for (int betaPost = 1; betaPost <= maxSampleSize - alphaPost + 1; betaPost++) {
                BetaDistribution betaDist = new BetaDistribution(alphaPost, betaPost);
                double lowerBound = betaDist.inverseCumulativeProbability((1 - credibilityLevel) / 2);
                double upperBound = betaDist.inverseCumulativeProbability(1 - (1 - credibilityLevel) / 2);

                if (lowerBound > decisionThreshold) {
                    lookupTable[alphaPost][betaPost] = 2; // Violated
                } else if (upperBound < decisionThreshold) {
                    lookupTable[alphaPost][betaPost] = 1; // Satisfied
                } else {
                    lookupTable[alphaPost][betaPost] = 0; // Inconclusive
                }
            }
        }
        return lookupTable;
    }

    public static void main(String[] args) {
        int maxSampleSize = 50;
        double credibilityLevel = 0.90;
        double decisionThreshold = 0.5;

        int[][] lookupTable = createLookupTable(maxSampleSize, credibilityLevel, decisionThreshold);

        // Display the lookup table
        for (int i = 1; i <= maxSampleSize; i++) {
            for (int j = 1; j <= maxSampleSize; j++) {
                if (i + j <= maxSampleSize) {
                    System.out.print(lookupTable[i][j]);
                }
            }
            System.out.println();
        }

        // Display a few entries for brevity
        for (int i = 1; i <= maxSampleSize; i++) {
            for (int j = 1; j <= maxSampleSize - i + 1; j++) {
                System.out.println("Alpha: " + i + ", Beta: " + j + ", Decision: " + lookupTable[i][j]);
            }
        }


    }
}
