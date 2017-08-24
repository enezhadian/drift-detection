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

    public static DILCAMatrix distanceMatrixFor(ImmutableList<ImmutableList<String>> database,
                                                int targetAttributeIndex) {
        // Find value domain for target attribute.
        ImmutableSet.Builder<String> domainBuilder = ImmutableSet.builder();
        for (ImmutableList<String> record : database) {
            domainBuilder.add(record.get(targetAttributeIndex));
        }
        ImmutableSet<String> domain = domainBuilder.build();

        // Find context attributes.
        int[] contextAttributeIndexes = contextAttributeIndexesFor(database, targetAttributeIndex);

        // Build distance matrix.
        DILCAMatrix distanceMatrix = new DILCAMatrix(domain);
        double distance;
        for (String firstValue : domain) {
            for (String secondValue : domain) {
                if (firstValue.compareTo(secondValue) < 0) {
                    distance = distanceFor(database, targetAttributeIndex, contextAttributeIndexes,
                            firstValue, secondValue);
                    distanceMatrix.set(firstValue, secondValue, distance);
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

    private static double cooccurrencesFor(ImmutableList<ImmutableList<String>> database,
                                           int targetAttributeIndex,
                                           int contextAttributeIndex) {
        // TODO: Implement this.
        Map<String, Map<String, Integer>> cooccurrencesMap = new HashMap<>();

        String contextValue, targetValue;
        Map<String, Integer> targetMap;
        for (ImmutableList<String> record : database) {
            contextValue = record.get(contextAttributeIndex);
            targetValue = record.get(targetAttributeIndex);

            // Increment co-occurrence count for context and target values.
            targetMap = cooccurrencesMap.getOrDefault(contextValue, new HashMap<>());
            targetMap.put(targetValue, targetMap.getOrDefault(targetValue, 0) + 1);
            cooccurrencesMap.put(contextValue, targetMap);
        }

        return 0;
    }

    // Make `DILCA` non-instantiatable.
    private DILCA() {}
}
