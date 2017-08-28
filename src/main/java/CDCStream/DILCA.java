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

    /*--------------------------------------------------------------------------*
     *                        STATIC MEMBERS AND METHODS                        *
     *--------------------------------------------------------------------------*/

    public static DILCA distanceMatrixFor(DatabaseStatistics statistics,
                                          int targetAttributeIndex) {
        System.out.print(".");
        System.out.flush();

        // Find context attributes.
        Set<Integer> contextAttributeIndexes = contextAttributeIndexesFor(statistics, targetAttributeIndex);

        // Build the distance matrix.
        int targetDomainSize = statistics.domainSize(targetAttributeIndex);
        double[][] distances = new double[targetDomainSize - 1][];
        for (int i = 0; i < distances.length; i++) {
            distances[i] = new double[targetDomainSize - i - 1];
        }

        int[][] cooccurrences;
        int[] valueCooccurrences;
        double[] valueDistances;

        double totalContextDomainSizes = 0;
        for (int attributeIndex : contextAttributeIndexes) {
            if (attributeIndex <= targetAttributeIndex) {
                cooccurrences = statistics.cooccurrencesFor(attributeIndex, targetAttributeIndex);
            } else {
                cooccurrences = statistics.cooccurrencesFor(targetAttributeIndex, attributeIndex);
            }

            // Calculate total sum of domain sizes for all attributes.
            totalContextDomainSizes += cooccurrences.length;

            // Calculate the sum of squared differences over all the values of current context attribute.
            for (int i = 0; i < cooccurrences.length; i++) {
                valueCooccurrences = cooccurrences[i];

                for (int j = 0; j < valueCooccurrences.length; j++) {
                    for (int k = j + 1; k < valueCooccurrences.length; k++) {
                        double difference = valueCooccurrences[j] - valueCooccurrences[k];
                        distances[j][k - j - 1] += difference * difference;
                    }
                }
            }
        }

        // Normalize sum of squared differences.
        for  (int i = 0; i < distances.length; i++) {
            valueDistances = distances[i];

            for (int j = 0; j < valueDistances.length; j++) {
                valueDistances[j] = Math.sqrt(valueDistances[j] / totalContextDomainSizes);
            }
        }

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

        int firstIndex, secondIndex;
        if (targetAttributeIndex < attributeIndex) {
            firstIndex = targetAttributeIndex;
            secondIndex = attributeIndex;
        } else {
            firstIndex = attributeIndex;
            secondIndex = targetAttributeIndex;
        }

        int[][] firstAttributeOccurrences = statistics.cooccurrencesFor(firstIndex, firstIndex);
        int[][] secondAttributeOccurrences = statistics.cooccurrencesFor(secondIndex, secondIndex);
        int[][] cooccurrences = statistics.cooccurrencesFor(firstIndex, secondIndex);

        double probability;

        double firstAttributeEntropy = 0;
        // Calculate target attribute's entropy.
        double firstAttributeTotalOccurrences = 0;
        for (int i = 0; i < firstAttributeOccurrences.length; i++) {
            firstAttributeTotalOccurrences += firstAttributeOccurrences[i][i];
        }
        for (int i = 0; i < firstAttributeOccurrences.length; i++) {
            probability = (double) firstAttributeOccurrences[i][i] / firstAttributeTotalOccurrences;
            firstAttributeEntropy -= probability * Math.log(probability) / log2;
        }

        double secondAttributeEntropy = 0;
        // Calculate attribute's entropy.
        double secondAttributeTotalOccurrences = 0;
        for (int i = 0; i < secondAttributeOccurrences.length; i++) {
            secondAttributeTotalOccurrences += secondAttributeOccurrences[i][i];
        }
        for (int i = 0; i < secondAttributeOccurrences.length; i++) {
            probability = (double) secondAttributeOccurrences[i][i] / secondAttributeTotalOccurrences;
            secondAttributeEntropy -= probability * Math.log(probability) / log2;
        }

        double attributeValueTotalOccurrences, conditionalEntropy, targetEntropy, attributeEnropy;
        if (targetAttributeIndex < attributeIndex) {
            targetEntropy = firstAttributeEntropy;
            attributeEnropy = secondAttributeEntropy;

            // Calculate conditional entropy of target attribute with respect to the given attribute.
            conditionalEntropy = 0;
            for (int i = 0; i < cooccurrences.length; i++) {
                for (int j = 0; j < cooccurrences[i].length; j++) {
                    attributeValueTotalOccurrences = secondAttributeOccurrences[j][j];
                    probability = (double) cooccurrences[i][j] / attributeValueTotalOccurrences;
                    conditionalEntropy -= probability * Math.log(probability) / log2;
                }
            }
        } else {
            targetEntropy = secondAttributeEntropy;
            attributeEnropy = firstAttributeEntropy;

            // Calculate conditional entropy of target attribute with respect to the given attribute.
            conditionalEntropy = 0;
            for (int i = 0; i < cooccurrences.length; i++) {
                attributeValueTotalOccurrences = firstAttributeOccurrences[i][i];
                for (int j = 0; j < cooccurrences[i].length; j++) {
                    probability = (double) cooccurrences[i][j] / attributeValueTotalOccurrences;
                    conditionalEntropy -= probability * Math.log(probability) / log2;
                }
            }
        }

        // Calculate symmetrical uncertainty.
        return (targetEntropy - conditionalEntropy) / (targetEntropy + attributeEnropy);
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
