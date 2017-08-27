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

    public static DILCA distanceMatrixFor(ImmutableList<ImmutableList<String>> database,
                                          int targetAttributeIndex) {
        // TODO: Implement this.
        return null;
//        // Find value domain for target attribute.
//        ImmutableSet.Builder<String> domainBuilder = ImmutableSet.builder();
//        for (ImmutableList<String> record : database) {
//            domainBuilder.add(record.get(targetAttributeIndex));
//        }
//        ImmutableSet<String> domain = domainBuilder.build();
//
//        // Build distance matrix.
//        DILCAMatrix distanceMatrix = new DILCAMatrix(domain);
//
//
//        // Find context attributes.
//        int[] contextAttributeIndexes = contextAttributeIndexesFor(database, targetAttributeIndex);
//
//        Map<String, Map<String, Integer>> cooccurrencesMap = new HashMap<>();
//        String contextValue, targetValue;
//        Map<String, Integer> targetMap;
//        double totalContextDomainSizes = 0;
//        double currentSum, difference;
//
//        for (int contextAttributeIndex : contextAttributeIndexes) {
//            // Clear co-occurrences map to use it for current context attribute.
//            cooccurrencesMap.clear();
//
//            // Count co-occurrences of context and target values.
//            for (ImmutableList<String> record : database) {
//                contextValue = record.get(contextAttributeIndex);
//                targetValue = record.get(targetAttributeIndex);
//
//                targetMap = cooccurrencesMap.getOrDefault(contextValue, new HashMap<>());
//                targetMap.put(targetValue, targetMap.getOrDefault(targetValue, 0) + 1);
//                cooccurrencesMap.put(contextValue, targetMap);
//            }
//
//            totalContextDomainSizes += cooccurrencesMap.size();
//
//            // Calculate the sum of squared differences over all the values of current context attribute.
//            for (String firstValue : domain) {
//                for (String secondValue : domain) {
//                    if (firstValue.compareTo(secondValue) < 0) {
//                        for (String value : cooccurrencesMap.keySet()) {
//                            targetMap = cooccurrencesMap.get(value);
//
//                            currentSum = distanceMatrix.get(firstValue, secondValue);
//                            difference = (targetMap.getOrDefault(firstValue, 0) -
//                                    targetMap.getOrDefault(secondValue, 0));
//                            distanceMatrix.set(firstValue, secondValue, currentSum + (difference * difference));
//                        }
//                    }
//                }
//            }
//        }
//
//        // Normalize sum of squared differences.
//        for (String firstValue : domain) {
//            for (String secondValue : domain) {
//                if (firstValue.compareTo(secondValue) < 0) {
//                    currentSum = distanceMatrix.get(firstValue, secondValue);
//                    distanceMatrix.set(firstValue, secondValue, Math.sqrt(currentSum / totalContextDomainSizes));
//                }
//            }
//        }
//
//        return distanceMatrix;
    }

    public static List<Map<String, Map<String, Integer>>> cooccurrences(ImmutableList<ImmutableList<String>> database,
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


    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    public double get(String firstValue, String secondValue) {
        // Get indexes corresponding to given values.
        int firstIndex = valueMap.getOrDefault(firstValue, -1);
        int secondIndex = valueMap.getOrDefault(secondValue, -1);

        if (firstIndex < 0 || secondIndex < 0) {
            throw new IndexOutOfBoundsException("At least one of the values are out of the domain.");
        }

        // Make sure `firstIndex` is the smaller index.
        if (firstIndex == secondIndex) {
            // Diagonal is always zero.
            return 0;
        } else if (firstIndex > secondIndex) {
            int temp = firstIndex;
            firstIndex = secondIndex;
            secondIndex = temp;
        }

        return distances[firstIndex][secondIndex - firstIndex];
    }

    private final ImmutableMap<String, Integer> valueMap;
    private final double[][] distances;

    private DILCA(Set<String> domain) {
        if (domain.size() == 0) {
            throw new IllegalArgumentException("Domain of values cannot be empty.");
        }

        // Store attribute value domain.
        ImmutableMap.Builder<String, Integer> mapBuilder = ImmutableMap.builder();
        int index = 0;
        for (String value : domain) {
            mapBuilder.put(value, index);
            index++;
        }
        this.valueMap = mapBuilder.build();

        // Create a two dimensional structure to store upper triangle of distance matrix.
        int size = this.valueMap.size() - 1;

        this.distances = new double[size][];
        for (int i = 0; i < size; i++) {
            this.distances[i] = new double[size - i];
        }
    }

    private void set(String firstValue, String secondValue, double distance) {
        // Get indexes corresponding to given values.
        int firstIndex = valueMap.getOrDefault(firstValue, -1);
        int secondIndex = valueMap.getOrDefault(secondValue, -1);

        if (firstIndex < 0 || secondIndex < 0) {
            throw new IndexOutOfBoundsException("At least one of the values are out of the domain.");
        } else if (firstIndex == secondIndex) {
            throw new IllegalArgumentException("Diagonal can't be set.");
        }

        // Make sure `firstIndex` is the smaller index.
        if (firstIndex > secondIndex) {
            int temp = firstIndex;
            firstIndex = secondIndex;
            secondIndex = temp;
        }

        distances[firstIndex][secondIndex - firstIndex] = distance;
    }

    private double normalizedSquaredSum() {
        double sum = 0, numValues = 0;
        for (double[] row : distances) {
            for (double value : row) {
                sum += value * value;
                numValues++;
            }
        }
        return sum / numValues;
    }

}
