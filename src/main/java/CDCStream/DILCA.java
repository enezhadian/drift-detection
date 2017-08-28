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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


class DILCA {

    /*--------------------------------------------------------------------------*
     *                        STATIC MEMBERS AND METHODS                        *
     *--------------------------------------------------------------------------*/

    // TODO: Make sure this is fast enough.
    public static DILCA distanceMatrixFor(ImmutableList<ImmutableList<String>> database,
                                          int targetAttributeIndex) {
        System.out.print(".");
        System.out.flush();

        if (database.size() == 0) {
            throw new IllegalArgumentException("Empty database.");
        }

        // Build the distance matrix.
        Map<String, Map<String, Double>> distances = new HashMap<>();

        // Find context attributes.
        Set<Integer> contextAttributeIndexes = contextAttributeIndexesFor(database, targetAttributeIndex);

        Map<String, Map<String, Integer>> cooccurrences = new HashMap<>();
        Map<String, Integer> valueCooccurrences;
        Map<String, Double> valueDistances;

        double totalContextDomainSizes = 0;
        for (int attributeIndex : contextAttributeIndexes) {
            cooccurrences.clear();
            calculateStatisticsFor(database, targetAttributeIndex, attributeIndex, null, null, cooccurrences);

            // Calculate total sum of domain sizes for all attributes.
            totalContextDomainSizes += cooccurrences.size();

            // Calculate the sum of squared differences over all the values of current context attribute.
            for (String attributeValue : cooccurrences.keySet()) {
                valueCooccurrences = cooccurrences.get(attributeValue);

                for (String firstValue : valueCooccurrences.keySet()) {
                    for (String secondValue : valueCooccurrences.keySet()) {
                        if (firstValue.compareTo(secondValue) < 0) {
                            valueDistances = distances.getOrDefault(firstValue, new HashMap<>());

                            double currentSum = valueDistances.getOrDefault(secondValue, 0.0);
                            double difference = valueCooccurrences.getOrDefault(firstValue, 0) -
                                    valueCooccurrences.getOrDefault(secondValue, 0);
                            valueDistances.put(secondValue, currentSum + (difference * difference));
                            distances.put(firstValue, valueDistances);
                        }
                    }
                }
            }
        }

        // Normalize sum of squared differences.
        for (String firstValue : distances.keySet()) {
            valueDistances = distances.get(firstValue);

            for (String secondValue : valueDistances.keySet()) {
                double currentSum = valueDistances.get(secondValue);
                valueDistances.put(secondValue, Math.sqrt(currentSum / totalContextDomainSizes));
            }
        }

        return new DILCA(distances);
    }

    private static final double log2 = Math.log(2);

    private static Set<Integer> contextAttributeIndexesFor(ImmutableList<ImmutableList<String>> database,
                                                           int targetAttributeIndex) {
        int numAttributes = database.get(0).size();
        List<Double> uncertainties = new ArrayList<>(numAttributes);
        List<Integer> indexes = new ArrayList<>(numAttributes);

        // Calculate attribute relevance.
        for (int i = 0; i < numAttributes; i++) {
            if (i != targetAttributeIndex) {
                indexes.add(i);
                uncertainties.add(symmetricalUncertainty(database, targetAttributeIndex, i));
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
                    if (-1 != secondAttribute && symmetricalUncertainty(database, firstAttribute, secondAttribute) <=
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

    private static double symmetricalUncertainty(ImmutableList<ImmutableList<String>> database,
                                                 int targetAttributeIndex,
                                                 int attributeIndex) {
        Map<String, Integer> targetOccurrences = new HashMap<>();
        Map<String, Integer> attributeOccurrences = new HashMap<>();
        Map<String, Map<String, Integer>> cooccurrences = new HashMap<>();

        calculateStatisticsFor(database, targetAttributeIndex, attributeIndex,
                targetOccurrences, attributeOccurrences, cooccurrences);

        double probability;
        Map<String, Integer> valueCoccurrences;

        double targetEntropy = 0;
        // Calculate target attribute's entropy.
        double totalTargetOccurrences = 0;
        for (String value : targetOccurrences.keySet()) {
            totalTargetOccurrences += targetOccurrences.get(value);
        }
        for (String value : targetOccurrences.keySet()) {
            probability = (double) targetOccurrences.get(value) / totalTargetOccurrences;
            targetEntropy -= probability * Math.log(probability) / log2;
        }

        double attributeEntropy = 0;
        // Calculate attribute's entropy.
        double totalAttributeOccurrences = 0;
        for (String value : attributeOccurrences.keySet()) {
            totalAttributeOccurrences += attributeOccurrences.get(value);
        }
        for (String value : attributeOccurrences.keySet()) {
            probability = (double) attributeOccurrences.get(value) / totalAttributeOccurrences;
            attributeEntropy -= probability * Math.log(probability) / log2;
        }

        double conditionalEntropy = 0;
        // Calculate conditional entropy of target attribute with respect to the given attribute.
        for (String attributeValue : cooccurrences.keySet()) {
            valueCoccurrences = cooccurrences.get(attributeValue);

            double totalValueCooccurrences = 0;
            for (String value : valueCoccurrences.keySet()) {
                totalValueCooccurrences += valueCoccurrences.get(value);
            }

            for (String targetValue : valueCoccurrences.keySet()) {
                probability = (double) valueCoccurrences.get(targetValue) / totalValueCooccurrences;
                conditionalEntropy -= probability * Math.log(probability) / log2;
            }
        }

        // Calculate symmetrical uncertainty.
        return (targetEntropy - conditionalEntropy) / (targetEntropy + attributeEntropy);
    }

    private static void calculateStatisticsFor(ImmutableList<ImmutableList<String>> database,
                                               int targetAttributeIndex,
                                               int attributeIndex,
                                               Map<String, Integer> outputTargetOccurrences,
                                               Map<String, Integer> outputAttributeOccurrences,
                                               Map<String, Map<String, Integer>> outputCooccurrences) {
        if (null == outputCooccurrences) {
            throw new IllegalArgumentException("`outputCoocurrences` cannot be null.");
        }

        outputCooccurrences.clear();
        if (null != outputTargetOccurrences) {
            outputTargetOccurrences.clear();
        }
        if (null != outputAttributeOccurrences) {
            outputAttributeOccurrences.clear();
        }

        String attributeValue, targetValue;
        Map<String, Integer> valueCooccurrences;

        // Count occurrences and co-occurrences of attribute values and values of target attribute.
        for (ImmutableList<String> record : database) {
            attributeValue = record.get(attributeIndex);
            targetValue = record.get(targetAttributeIndex);

            valueCooccurrences = outputCooccurrences.getOrDefault(attributeValue, new HashMap<>());
            valueCooccurrences.put(targetValue, valueCooccurrences.getOrDefault(targetValue, 0) + 1);
            outputCooccurrences.put(attributeValue, valueCooccurrences);

            if (null != outputTargetOccurrences) {
                outputTargetOccurrences.put(attributeValue,
                        outputTargetOccurrences.getOrDefault(attributeValue, 0) + 1);
            }

            if (null != outputAttributeOccurrences) {
                outputAttributeOccurrences.put(attributeValue,
                        outputAttributeOccurrences.getOrDefault(attributeValue, 0) + 1);
            }
        }
    }

    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    public ImmutableSet<String> domain() {
        return domain;
    }

    public double get(String firstValue, String secondValue) {
        int comparison = firstValue.compareTo(secondValue);
        if (0 == comparison) {
            // Make sure `firstValue` is a valid value.
            distances.get(firstValue);
            return 0;
        }

        // Make sure `firstValue` is smaller than `secondValue`.
        if (0 > comparison) {
            String temp = firstValue;
            firstValue = secondValue;
            secondValue = temp;
        }

        return distances.get(firstValue).get(secondValue);
    }

    public double normalizedSquaredSumRoot() {
        double sum = 0;
        Map<String, Double> valueDistances;

        for (String firstValue : distances.keySet()) {
            valueDistances = distances.get(firstValue);

            for (String secondValue : valueDistances.keySet()) {
                sum += valueDistances.get(secondValue);
            }
        }

        if (0 == sum) {
            return 0;
        } else {
            return (2 * Math.sqrt(sum)) / (domain.size() * (domain.size() - 1));
        }
    }

    private Map<String, Map<String, Double>> distances;
    private ImmutableSet<String> domain;

    private DILCA(Map<String, Map<String, Double>> distances) {
        this.distances = distances;

        ImmutableSet.Builder<String> domainBuilder = ImmutableSet.<String>builder().addAll(distances.keySet());
        for (String value : distances.keySet()) {
            domainBuilder.addAll(distances.get(value).keySet());
        }
        this.domain = domainBuilder.build();
    }

}
