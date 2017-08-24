/*
 *      CDDA/DriftDetector.java
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

package CDDA;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;

import DataStreamReader.CategoricalRecordStreamReader;
import com.google.common.collect.Sets;


public class DriftDetector {

    public DriftDetector(CategoricalRecordStreamReader stream, int blockSize, double minChangeDegree) {
        this.stream = stream;
        this.blockSize = blockSize;
        this.minChangeDegree = minChangeDegree;
    }

    public void run() {
        try {
            ImmutableList<ImmutableList<String>> lastBlock, currentBlock;

            lastBlock = stream.head(blockSize);
            stream.discard(lastBlock.size());

            double changeDegree;
            String color;
            while (true) {
                // Read a block from the stream.
                currentBlock = stream.head(blockSize);
                stream.discard(currentBlock.size());

                changeDegree = changeDegreeFor(lastBlock, currentBlock);
                if (changeDegree >= minChangeDegree) {
                    color = "\033[1;32m";
                } else {
                    color = "\033[1;31m";
                }

                // TODO: Report concept drift in more useful way.
                System.out.println(color + "*** CHANGE: "  + changeDegree + " ***\033[0m");

                lastBlock = currentBlock;
            }
        } catch (NoSuchElementException e) {
            System.out.println("Done.");
        }
    }

    private final CategoricalRecordStreamReader stream;
    private final int blockSize;
    private final double minChangeDegree;

    private double changeDegreeFor(ImmutableList<ImmutableList<String>> firstBlock,
                                   ImmutableList<ImmutableList<String>> secondBlock) {
        if (firstBlock.size() == 0 || secondBlock.size() == 0) {
            throw new IllegalArgumentException("Blocks should not be empty.");
        }

        int numAttributes = firstBlock.get(0).size();
        if (secondBlock.get(0).size() != numAttributes) {
            throw new IllegalArgumentException("Number of features should match.");
        }

        double totalNewConceptEmergingDegree = 0;
        double totalOldConceptFadingDegree = 0;

        Map<String, Integer> firstEquivalents = new HashMap<>();
        Map<String, Integer> secondEquivalents = new HashMap<>();

        int firstCount, secondCount;
        double firstLowerApprox, firstUpperApprox, secondLowerApprox, secondUpperApprox;

        for (int attribute = 0; attribute < numAttributes; attribute++) {
            firstEquivalents.clear();
            secondEquivalents.clear();

            // Calculate size of each equivalent class for the first block.
            for (ImmutableList<String> record : firstBlock) {
                String value = record.get(attribute);
                firstEquivalents.put(value, firstEquivalents.getOrDefault(value, 0) + 1);
            }

            // Calculate size of each equivalent class for the second block.
            for (ImmutableList<String> record : secondBlock) {
                String value = record.get(attribute);
                secondEquivalents.put(value, secondEquivalents.getOrDefault(value, 0) + 1);
            }

            // Calculate the lower and upper approximations of the blocks for `attribute`.
            firstLowerApprox = firstUpperApprox = secondLowerApprox = secondUpperApprox = 0;
            for (String value : Sets.union(firstEquivalents.keySet(), secondEquivalents.keySet())) {
                firstCount = firstEquivalents.getOrDefault(value, 0);
                secondCount = secondEquivalents.getOrDefault(value, 0);

                if (firstCount > 0) {
                    if (secondCount > 0) {
                        firstUpperApprox += firstCount;
                        secondUpperApprox += secondCount;
                    } else {
                        firstLowerApprox += firstCount;
                    }
                } else if (secondCount > 0) {
                    secondLowerApprox += secondCount;
                }
            }

            // Calculated old concept fading and new concept emerging degrees for `attribute`.
            totalOldConceptFadingDegree += (firstLowerApprox / firstUpperApprox);
            totalNewConceptEmergingDegree += (secondLowerApprox / secondUpperApprox);
        }

        // Calculate and return change degree.
        return (totalNewConceptEmergingDegree + totalOldConceptFadingDegree) / (2 * numAttributes);
    }

}
