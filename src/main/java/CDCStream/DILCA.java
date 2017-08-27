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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


class DILCA {

    /*--------------------------------------------------------------------------*
     *                        STATIC MEMBERS AND METHODS                        *
     *--------------------------------------------------------------------------*/

    // TODO: Make sure this is fast enough.
    public static DILCA distanceMatrixFor(ImmutableList<ImmutableList<String>> database,
                                          int targetAttributeIndex) {
        if (database.size() == 0) {
            throw new IllegalArgumentException("Empty database.");
        }

        int numAttributes = database.get(0).size();
        // Calculate the co-occurrences of target values with attribute values.
        List<Map<String, Map<String, Integer>>> cooccurrences = cooccurrencesIn(
                database, numAttributes, targetAttributeIndex);


        // Build the distance matrix.
        Map<String, Map<String, Double>> distances = new HashMap<>();

        // Find context attributes.
        int[] contextAttributeIndexes = contextAttributeIndexesFor(cooccurrences);

        Map<String, Map<String, Integer>> attributeCooccurrences;
        Map<String, Integer> valueCooccurrences;
        Map<String, Double> valueDistances;

        double totalContextDomainSizes = 0;
        for (int attributeIndex : contextAttributeIndexes) {
            attributeCooccurrences = cooccurrences.get(attributeIndex);

            // Calculate total sum of domain sizes for all attributes.
            totalContextDomainSizes += attributeCooccurrences.size();

            // Calculate the sum of squared differences over all the values of current context attribute.
            for (String attributeValue : attributeCooccurrences.keySet()) {
                valueCooccurrences = attributeCooccurrences.get(attributeValue);

                for (String firstValue : valueCooccurrences.keySet()) {
                    for (String secondValue : valueCooccurrences.keySet()) {
                        if (firstValue.compareTo(secondValue) < 0) {
                            valueDistances = distances.getOrDefault(firstValue, new HashMap<>());

                            double currentSum = valueDistances.getOrDefault(secondValue, 0.0);
                            double difference = valueCooccurrences.getOrDefault(firstValue, 0) -
                                    valueCooccurrences.getOrDefault(secondValue, 0);
                            valueDistances.put(secondValue, currentSum + (difference * difference));
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

    private static List<Map<String, Map<String, Integer>>> cooccurrencesIn(ImmutableList<ImmutableList<String>> database,
                                                                           int numAttributes,
                                                                           int targetAttributeIndex) {
        // Data structure holding the co-occurrences of target values and attribute values for all the attributes.
        List<Map<String, Map<String, Integer>>> cooccurrences = new ArrayList<>(numAttributes);
        for (int i = 0; i < numAttributes; i++) {
            if (i != targetAttributeIndex) {
                cooccurrences.set(i, new HashMap<>());
            }
        }

        String attributeValue, targetValue;
        Map<String, Map<String, Integer>> cooccurrencesMap;
        Map<String, Integer> targetMap;

        // Count co-occurrences of target values and values of the current attribute.
        for (ImmutableList<String> record : database) {
            for (int i = 0; i < numAttributes; i++) {
                if (i != targetAttributeIndex) {
                    cooccurrencesMap = cooccurrences.get(i);

                    attributeValue = record.get(i);
                    targetValue = record.get(targetAttributeIndex);

                    targetMap = cooccurrencesMap.getOrDefault(attributeValue, new HashMap<>());
                    targetMap.put(targetValue, targetMap.getOrDefault(targetValue, 0) + 1);
                    cooccurrencesMap.put(attributeValue, targetMap);
                }
            }
        }

        return cooccurrences;
    }

    private static int[] contextAttributeIndexesFor(List<Map<String, Map<String, Integer>>> cooccurrences) {
        // TODO: Implement this.
        return null;
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

        return (2 * Math.sqrt(sum)) / (domain.size() * (domain.size() - 1));
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
