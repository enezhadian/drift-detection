/*
 *      CDCStream/DILCA.java
 *      Drift Detection
 *
 *  Copyright 2017 Ehsan Nezhadian
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package CDCStream;

import java.util.*;

import com.google.common.collect.ImmutableSet;


public class DILCA {

    public static long time1 = 0;
    public static long time2 = 0;
    public static long time3 = 0;

    public static long time4 = 0;
    public static long time5 = 0;
    public static long time6 = 0;

    /*--------------------------------------------------------------------------*
     *                        STATIC MEMBERS AND METHODS                        *
     *--------------------------------------------------------------------------*/

    // TODO: Store every retrieved attribute in a local variable.
    public static DILCA distanceMatrixFor(DatabaseStatistics statistics,
                                          int targetAttributeIndex) {
//        System.out.print(".");
//        System.out.flush();

        long start;
        System.out.println(time1 + " " + time2 + " " + time3 + " " + time4 + " " + time5 + " " + time6);

        // Find context attributes.
        Set<Integer> contextAttributeIndexes = contextAttributeIndexesFor(statistics, targetAttributeIndex);

        int numAttributes = statistics.numAttributes();
        int targetDomainSize = statistics.domainSize(targetAttributeIndex);

        // Build the distance matrix.
        double[][] distances = new double[targetDomainSize - 1][];
        for (int i = 0; i < distances.length; i++) {
            distances[i] = new double[targetDomainSize - i - 1];
        }

        int[][] cooccurrences;
        double[] valueDistances;

        // TODO: Make this part faster.
        for (int attributeIndex : contextAttributeIndexes) {
            start = System.currentTimeMillis();
            // TODO: Also check the other order.
            cooccurrences = statistics.cooccurrencesFor(attributeIndex, targetAttributeIndex);
            // Calculate the sum of squared differences over all the values of current context attribute.
            for (int i = 0; i < cooccurrences.length; i++) {
                for (int j = 0; j < targetDomainSize; j++) {
                    for (int k = j + 1; k < targetDomainSize; k++) {
                        double difference = cooccurrences[i][j] - cooccurrences[i][k];
                        distances[j][k - j - 1] += difference * difference;
                    }
                }
            }
            time1 += System.currentTimeMillis() - start;
        }

        start = System.currentTimeMillis();
        // Calculate total sum of domain sizes for all attributes.
        double totalContextDomainSizes = 0;
        for (int i = 0; i < numAttributes; i++) {
            totalContextDomainSizes += statistics.domainSize(i);
        }

        // Normalize sum of squared differences.
        for  (int i = 0; i < distances.length; i++) {
            valueDistances = distances[i];
            for (int j = 0; j < valueDistances.length; j++) {
                valueDistances[j] = Math.sqrt(valueDistances[j] / totalContextDomainSizes);
            }
        }
        time3 += System.currentTimeMillis() - start;

        return new DILCA(distances);
    }

    private static final double log2 = Math.log(2);

    private static Set<Integer> contextAttributeIndexesFor(DatabaseStatistics statistics,
                                                           int targetAttributeIndex) {
        int numAttributes = statistics.numAttributes();
        List<Double> uncertainties = new ArrayList<>(numAttributes);
        List<Integer> indexes = new ArrayList<>(numAttributes);

        // Calculate attribute relevance.
        for (int i = 0; i < numAttributes; i++) {
            if (i != targetAttributeIndex) {
                indexes.add(i);
                uncertainties.add(symmetricalUncertainty(statistics, targetAttributeIndex, i));
            } else {
                uncertainties.add(null);
            }
        }

        // Sort indexes in descending order based on their corresponding symmetrical uncertainty.
        indexes.sort((i, j) -> (int) Math.signum(uncertainties.get(i) - uncertainties.get(j)));

        // Remove redundant attributes.
        int firstAttribute, secondAttribute;
        for (int i = 0; i < indexes.size(); i++) {
            firstAttribute = indexes.get(i);
            if (-1 != firstAttribute) {
                for (int j = i; j < indexes.size(); j++) {
                    secondAttribute = indexes.get(j);
                    if (-1 != secondAttribute && symmetricalUncertainty(statistics, firstAttribute, secondAttribute) <=
                            uncertainties.get(secondAttribute)) {
                        indexes.set(j, -1);
                    }
                }
            }
        }

        ImmutableSet.Builder<Integer> contextBuilder = ImmutableSet.builder();
        for (int i : indexes) {
            if (i != -1) {
                contextBuilder.add(i);
            }
        }
        return contextBuilder.build();
    }

    private static double symmetricalUncertainty(DatabaseStatistics statistics,
                                                 int targetAttributeIndex,
                                                 int attributeIndex) {
        if (targetAttributeIndex == attributeIndex) {
            return 0;
        }

        long start;

        int[][] targetOccurrences = statistics.cooccurrencesFor(targetAttributeIndex, targetAttributeIndex);
        int[][] attributeOccurrences = statistics.cooccurrencesFor(attributeIndex, attributeIndex);
        /* int[][] cooccurrences = statistics.cooccurrencesFor(targetAttributeIndex, attributeIndex); */
        int[][] cooccurrences = statistics.cooccurrencesFor(attributeIndex, targetAttributeIndex);

        double probability;

        start = System.currentTimeMillis();
        double targetEntropy = 0;
        // Calculate target attribute's entropy.
        double targetTotalOccurrences = 0;
        for (int i = 0; i < targetOccurrences.length; i++) {
            targetTotalOccurrences += targetOccurrences[i][i];
        }
        for (int i = 0; i < targetOccurrences.length; i++) {
            probability = (double) targetOccurrences[i][i] / targetTotalOccurrences;
            targetEntropy -= probability * Math.log(probability) / log2;
        }
        time4 += System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        double attributeEntropy = 0;
        // Calculate attribute's entropy.
        double attributeTotalOccurrences = 0;
        for (int i = 0; i < attributeOccurrences.length; i++) {
            attributeTotalOccurrences += attributeOccurrences[i][i];
        }
        for (int i = 0; i < attributeOccurrences.length; i++) {
            probability = (double) attributeOccurrences[i][i] / attributeTotalOccurrences;
            attributeEntropy -= probability * Math.log(probability) / log2;
        }
        time5 += System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        /* // Calculate conditional entropy of target attribute with respect to the given attribute.
        double conditionalEntropy = 0;
        for (int i = 0; i < cooccurrences.length; i++) {
            double attributeValueTotalOccurrences = targetOccurrences[i][i];
            for (int j = 0; j < cooccurrences[i].length; j++) {
                probability = (double) cooccurrences[i][j] / attributeValueTotalOccurrences;
                conditionalEntropy -= probability * Math.log(probability) / log2;
            }
        } */
        // Calculate conditional entropy of target attribute with respect to the given attribute.
        // TODO: Make this part faster.
        double conditionalEntropy = 0;
        for (int i = 0; i < cooccurrences.length; i++) {
            for (int j = 0; j < cooccurrences[i].length; j++) {
                double attributeValueTotalOccurrences = attributeOccurrences[i][i];
                probability = (double) cooccurrences[i][j] / attributeValueTotalOccurrences;
                conditionalEntropy -= probability * Math.log(probability) / log2;
            }
        }
        time6 += System.currentTimeMillis() - start;

        // Calculate symmetrical uncertainty.
        return (targetEntropy - conditionalEntropy) / (targetEntropy + attributeEntropy);
    }

    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    public double normalizedSquaredSumRoot() {
        double sum = 0;
        double[] valueDistances;

        for (int i = 0; i < distances.length; i++) {
            valueDistances = distances[i];

            for (int j = 0; j < valueDistances.length; j++) {
                sum += valueDistances[j];
            }
        }

        if (0 == sum) {
            return 0;
        } else {
            return (2 * Math.sqrt(sum)) / (distances.length * (distances.length - 1));
        }
    }

    private double[][] distances;

    private DILCA(double[][] distances) {
        this.distances = distances;
    }

}
