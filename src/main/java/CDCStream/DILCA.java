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


class DILCA {

    /*--------------------------------------------------------------------------*
     *                        STATIC MEMBERS AND METHODS                        *
     *--------------------------------------------------------------------------*/

    // TODO: Store every retrieved attribute in a local variable.
    static DILCA distanceMatrixFor(DatabaseStatistics statistics,
                                          int targetAttributeIndex) {
        // Find context attributes.
        Set<Integer> contextAttributeIndexes = contextAttributeIndexesFor(statistics, targetAttributeIndex);

        int targetDomainSize = statistics.domainSize(targetAttributeIndex);

        // Build the distance matrix.
        double[][] distances = new double[targetDomainSize - 1][];
        for (int i = 0; i < distances.length; i++) {
            distances[i] = new double[targetDomainSize - i - 1];
        }

        int[][] cooccurrences;
        double[] valueDistances;

        for (int attributeIndex : contextAttributeIndexes) {
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
        }

        // Calculate total sum of domain sizes for all attributes.
        double totalContextDomainSizes = 0;
        for (int attributeIndex : contextAttributeIndexes) {
            totalContextDomainSizes += statistics.domainSize(attributeIndex);
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
        Map<Integer, Double> uncertainties = new HashMap<>(numAttributes);
        List<Integer> indexes = new ArrayList<>(numAttributes);

        // Calculate attribute relevance.
        for (int i = 0; i < numAttributes; i++) {
            if (i != targetAttributeIndex) {
                indexes.add(i);
                uncertainties.put(i, symmetricalUncertainty(statistics, targetAttributeIndex, i));
            }
        }

        // Sort indexes in descending order based on their corresponding symmetrical uncertainty.
        indexes.sort((i, j) -> (int) Math.signum(uncertainties.get(j) - uncertainties.get(i)));

        // Remove redundant attributes.
        int firstAttribute, secondAttribute;
        for (int i = 0; i < indexes.size(); i++) {
            firstAttribute = indexes.get(i);
            if (-1 != firstAttribute) {
                for (int j = i + 1; j < indexes.size(); j++) {
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

        int[][] targetOccurrences = statistics.cooccurrencesFor(targetAttributeIndex, targetAttributeIndex);
        int[][] attributeOccurrences = statistics.cooccurrencesFor(attributeIndex, attributeIndex);
        int[][] cooccurrences = statistics.cooccurrencesFor(attributeIndex, targetAttributeIndex);

        double probability, occurrences, attributeValueTotalOccurrences;

        double targetEntropy = 0;
        // Calculate target attribute's entropy.
        double targetTotalOccurrences = 0;
        for (int i = 0; i < targetOccurrences.length; i++) {
            targetTotalOccurrences += targetOccurrences[i][i];
        }
        for (int i = 0; i < targetOccurrences.length; i++) {
            occurrences = targetOccurrences[i][i];
            if (0 == occurrences) {
                continue;
            }
            probability = occurrences / targetTotalOccurrences;
            targetEntropy -= probability * Math.log(probability) / log2;
        }

        double attributeEntropy = 0;
        // Calculate attribute's entropy.
        double attributeTotalOccurrences = 0;
        for (int i = 0; i < attributeOccurrences.length; i++) {
            attributeTotalOccurrences += attributeOccurrences[i][i];
        }
        for (int i = 0; i < attributeOccurrences.length; i++) {
            occurrences = attributeOccurrences[i][i];
            if (0 == occurrences) {
                continue;
            }
            probability = occurrences / attributeTotalOccurrences;
            attributeEntropy -= probability * Math.log(probability) / log2;
        }

        // Calculate conditional entropy of target attribute with respect to the given attribute.
        double conditionalEntropy = 0;
        for (int i = 0; i < cooccurrences.length; i++) {
            double currentValueEntropy = 0;
            attributeValueTotalOccurrences = attributeOccurrences[i][i];
            for (int j = 0; j < cooccurrences[i].length; j++) {
                occurrences = cooccurrences[i][j];
                if (0 == occurrences) {
                    continue;
                }
                probability = occurrences / attributeValueTotalOccurrences;
                currentValueEntropy -= probability * Math.log(probability) / log2;
            }
            conditionalEntropy += (attributeValueTotalOccurrences / attributeTotalOccurrences) * currentValueEntropy;
        }

        // Calculate symmetrical uncertainty.
        if (0 == targetEntropy && 0 == attributeEntropy) {
            return 0;
        } else {
            return 2 * (targetEntropy - conditionalEntropy) / (targetEntropy + attributeEntropy);
        }
    }

    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    double normalizedSquaredSumRoot() {
        double sum = 0;

        for (int i = 0; i < distances.length; i++) {
            for (int j = 0; j < distances[i].length; j++) {
                sum += distances[i][j] * distances[i][j];
            }
        }

        if (0 == sum) {
            return 0;
        } else {
            return (2 * Math.sqrt(sum)) / (distances.length * (distances.length + 1));
        }
    }

    double[][] distances;

    private DILCA(double[][] distances) {
        this.distances = distances;
    }

}
