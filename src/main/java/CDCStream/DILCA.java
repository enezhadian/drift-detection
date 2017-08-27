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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


class DILCA {

    // TODO: Make sure this is fast enough because at least it does not seem so.
    public static DILCAMatrix distanceMatrixFor(ImmutableList<ImmutableList<String>> database,
                                                int targetAttributeIndex) {
        // Find value domain for target attribute.
        ImmutableSet.Builder<String> domainBuilder = ImmutableSet.builder();
        for (ImmutableList<String> record : database) {
            domainBuilder.add(record.get(targetAttributeIndex));
        }
        ImmutableSet<String> domain = domainBuilder.build();

        // Build distance matrix.
        DILCAMatrix distanceMatrix = new DILCAMatrix(domain);


        // Find context attributes.
        int[] contextAttributeIndexes = contextAttributeIndexesFor(database, targetAttributeIndex);

        Map<String, Map<String, Integer>> cooccurrencesMap = new HashMap<>();
        String contextValue, targetValue;
        Map<String, Integer> targetMap;
        double totalContextDomainSizes = 0;
        double currentSum, difference;

        for (int contextAttributeIndex : contextAttributeIndexes) {
            // Clear co-occurrences map to use it for current context attribute.
            cooccurrencesMap.clear();

            // Count co-occurrences of context and target values.
            for (ImmutableList<String> record : database) {
                contextValue = record.get(contextAttributeIndex);
                targetValue = record.get(targetAttributeIndex);

                targetMap = cooccurrencesMap.getOrDefault(contextValue, new HashMap<>());
                targetMap.put(targetValue, targetMap.getOrDefault(targetValue, 0) + 1);
                cooccurrencesMap.put(contextValue, targetMap);
            }

            totalContextDomainSizes += cooccurrencesMap.size();

            // Calculate the sum of squared differences over all the values of current context attribute.
            for (String firstValue : domain) {
                for (String secondValue : domain) {
                    if (firstValue.compareTo(secondValue) < 0) {
                        for (String value : cooccurrencesMap.keySet()) {
                            targetMap = cooccurrencesMap.get(value);

                            currentSum = distanceMatrix.get(firstValue, secondValue);
                            difference = (targetMap.getOrDefault(firstValue, 0) -
                                    targetMap.getOrDefault(secondValue, 0));
                            distanceMatrix.set(firstValue, secondValue, currentSum + (difference * difference));
                        }
                    }
                }
            }
        }

        // Normalize sum of squared differences.
        for (String firstValue : domain) {
            for (String secondValue : domain) {
                if (firstValue.compareTo(secondValue) < 0) {
                    currentSum = distanceMatrix.get(firstValue, secondValue);
                    distanceMatrix.set(firstValue, secondValue, Math.sqrt(currentSum / totalContextDomainSizes));
                }
            }
        }

        return distanceMatrix;
    }

    private static int[] contextAttributeIndexesFor(ImmutableList<ImmutableList<String>> database,
                                                    int targetAttributeIndex) {
        // TODO: Implement this.
        return null;
    }

    private static double symmetricalUncertainty(ImmutableList<ImmutableList<String>> database,
                                                 int targetAttributeIndex,
                                                 int attributeIndex) {
        // TODO: Implement this.
        return 0;
    }

    // Make `DILCA` non-instantiatable.
    private DILCA() {}
}
