/*
 *      CDCStream/DatabaseStatistics.java
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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class DatabaseStatistics {

    // TODO: Make this faster.
    DatabaseStatistics(ImmutableList<ImmutableList<String>> database) {
        if (database.size() == 0) {
            throw new IllegalArgumentException("Database cannot be empty.");
        }

        this.numAttributes = database.get(0).size();

        this.attributeDomains = new ArrayList<>(numAttributes);
        for (int i = 0; i < numAttributes; i++) {
            attributeDomains.add(new HashMap<>());
        }

        // Find domain of all attributes.
        int[] nextIndex = new int[numAttributes];
        for (ImmutableList<String> record : database) {
            for (int i = 0; i < numAttributes; i++) {
                String value = record.get(i);
                if (!attributeDomains.get(i).containsKey(value)) {
                    attributeDomains.get(i).put(value, nextIndex[i]);
                    nextIndex[i]++;
                }
            }
        }

        // Initialize co-occurrences data structure with zero.
        int firstDomainSize, secondDomainSize;

        cooccurrences = new int[numAttributes][][][];
        for (int i = 0; i < numAttributes; i++) {
            cooccurrences[i] = new int[numAttributes][][];
            firstDomainSize = attributeDomains.get(i).size();

            for (int j = 0; j < numAttributes; j++) {
                cooccurrences[i][j] = new int[firstDomainSize][];
                secondDomainSize = attributeDomains.get(j).size();

                for (int k = 0; k < firstDomainSize; k++) {
                    cooccurrences[i][j][k] = new int[secondDomainSize];
                }
            }
        }

        // Count co-occurrences.
        int firstIndex, secondIndex;

        for (ImmutableList<String> record : database) {
            for (int i = 0; i < numAttributes; i++) {
                for (int j = 0; j < numAttributes; j++) {
                    firstIndex = attributeDomains.get(i).get(record.get(i));
                    secondIndex = attributeDomains.get(j).get(record.get(j));
                    this.cooccurrences[i][j][firstIndex][secondIndex]++;
                }
            }
        }
    }

    int[][] cooccurrencesFor(int lesserAttributeIndex, int greaterAttributeIndex) {
        return cooccurrences[lesserAttributeIndex][greaterAttributeIndex];
    }

    int numAttributes() {
        return numAttributes;
    }

    int domainSize(int attributeIndex) {
        return attributeDomains.get(attributeIndex).size();
    }

    private final int numAttributes;
    private final List<Map<String, Integer>> attributeDomains;
    private final int[][][][] cooccurrences;

}

