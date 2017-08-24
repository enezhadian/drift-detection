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

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


class DILCAMatrix {

    public DILCAMatrix(Set<String> domain) {
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

    public ImmutableSet<String> domain() {
        return valueMap.keySet();
    }

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

    public void set(String firstValue, String secondValue, double distance) {
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

    private final ImmutableMap<String, Integer> valueMap;
    private final double[][] distances;

}
