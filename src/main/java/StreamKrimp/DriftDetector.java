/*
 *      StreamKrimp/DriftDetector.java
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

package StreamKrimp;

import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import DataStreamReader.ItemsetStreamReader;


public class DriftDetector {

    public DriftDetector(ItemsetStreamReader stream, int blockSize, double minSupport,
                         double maxImprovementRate, double minCodeTableDifference,
                         int numSamples, double leaveOut) throws IllegalArgumentException {

        if (2 * (int)(leaveOut * numSamples) > numSamples) {
            throw new IllegalArgumentException();
        }

        this.stream = stream;
        this.blockSize = blockSize;
        this.minSupport = minSupport;
        this.maxImprovementRate = maxImprovementRate;
        this.minCodeTableDifference = minCodeTableDifference;
        this.numSamples = numSamples;
        this.leaveOut = leaveOut;
        this.sampleLengths = new double[numSamples];
    }

    public void run() {
        CodeTable currentCodeTable;

        // Create a code table for the first block of data stream.
        findCodeTable();
        currentCodeTable = convergedCodeTable;

        boolean doSample = true;
        try {
            while (true) {
                discardBlocksConformingTo(doSample);

                findCodeTable();
                double difference = convergedCodeTable.differenceWith(
                        currentCodeTable, convergedHead);

                if (difference >= minCodeTableDifference) {
                    // TODO: Report concept drift in more useful way.
                    System.out.println("++++ DRIFT DETECTED (DIFF: "  + difference + ") ++++");
                    currentCodeTable = convergedCodeTable;
                    stream.discard(convergedHead.size());
                    doSample = true;
                } else {
                    System.out.println("---- NO DRIFT DETECTED (DIFF: "  + difference + ") ----");
                    stream.discard(blockSize);
                    doSample = false;
                }
            }
        } catch (NoSuchElementException e) {
            System.out.println("Done.");
        }
    }


    private final ItemsetStreamReader stream;
    private final int blockSize;
    private final double minSupport;
    private final double maxImprovementRate;
    private final double minCodeTableDifference;
    private final int numSamples;
    private final double leaveOut;
    private final double[] sampleLengths;

    // Remember last code table and the corresponding head of stream found by `findCodeTable`.
    private CodeTable convergedCodeTable;
    private ImmutableList<ImmutableSet<String>> convergedHead;

    private void findCodeTable() {
        int sliceSize = blockSize;

        convergedHead = stream.head(sliceSize);
        convergedCodeTable = CodeTable.optimalFor(convergedHead, stream.items(), minSupport);

        CodeTable newCodeTable;
        double ir, len, newLen;
        do {
            sliceSize += blockSize;
            convergedHead = stream.head(sliceSize);
            newCodeTable = CodeTable.optimalFor(convergedHead, stream.items(), minSupport);

            len = convergedCodeTable.totalLengthOf(convergedHead);
            newLen = newCodeTable.totalLengthOf(convergedHead);
            ir = Math.abs(len - newLen) / len;

            convergedCodeTable = newCodeTable;
        } while (ir > maxImprovementRate);
    }

    private void discardBlocksConformingTo(boolean doSample) {
        // Sample head and calculate their encoded length.
        if (doSample) {
            Random random = new Random(System.currentTimeMillis());

            int sampleSize = convergedHead.size() < blockSize ? convergedHead.size() : blockSize;
            Set<Integer> indexes = new HashSet<>(sampleSize);
            for (int i = 0; i < numSamples; i++) {
                indexes.clear();
                while (indexes.size() < sampleSize) {
                    indexes.add(random.nextInt(convergedHead.size()));
                }

                ImmutableList.Builder<ImmutableSet<String>> sampleBuilder = ImmutableList.builder();
                for (int index : indexes) {
                    sampleBuilder.add(convergedHead.get(index));
                }
                sampleLengths[i] = convergedCodeTable.totalLengthOf(sampleBuilder.build());
            }

            Arrays.sort(sampleLengths);
        }

        // Find minimum and maximum lengths ignoring top and botton `leaveOut` percent of them.
        int ignoreOnEachSize = (int)(numSamples * leaveOut);
        double minLength = sampleLengths[ignoreOnEachSize];
        double maxLength = sampleLengths[numSamples - ignoreOnEachSize - 1];

        // Discard conforming blocks.
        double blockLength;
        ImmutableList<ImmutableSet<String>> block;
        while (true) {
            block = stream.head(blockSize);
            if (block.size() == 0) {
                break;
            }

            blockLength = convergedCodeTable.totalLengthOf(block);
            if (minLength <= blockLength && blockLength <= maxLength) {
                System.out.println("Skipping a block.");
                stream.discard(blockSize);
            } else {
                break;
            }
        }
    }

}
