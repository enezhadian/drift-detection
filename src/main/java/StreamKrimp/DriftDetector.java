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

import javax.swing.text.AbstractDocument.LeafElement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import DataStreamReader.ItemsetStreamReader;


public class DriftDetector {

    public DriftDetector(ItemsetStreamReader stream, int blockSize, int minFrequency,
                         double maxImprovementRate, double minCodeTableDifference,
                         int numSamples, double nSigma) throws IllegalArgumentException {

        if (nSigma <= 0) {
            throw new IllegalArgumentException();
        }

        this.stream = stream;
        this.blockSize = blockSize;
        this.minFrequency = minFrequency;
        this.maxImprovementRate = maxImprovementRate;
        this.minCodeTableDifference = minCodeTableDifference;
        this.numSamples = numSamples;
        this.nSigma = nSigma;
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

                int lastRowOfCurrentCodeTable = stream.read;

                findCodeTable();
                double len = currentCodeTable.totalLengthOf(convergedHead);
                double newLen = convergedCodeTable.totalLengthOf(convergedHead);
                double difference = (len - newLen) / newLen;

                if (difference >= minCodeTableDifference) {
                    // TODO: Report concept drift in more useful way.
                    System.out.print("\033[1;32m*** DIFF: "  + difference);
                    currentCodeTable = convergedCodeTable;
                    stream.discard(convergedHead.size());
                    doSample = true;
                } else {
                    if (difference < 0) {
                        System.out.print("\033[1;31m*** DIFF: " + difference);
                    } else {
                        System.out.print("\033[1;33m*** DIFF: " + difference);
                    }

                    stream.discard(blockSize);
                    doSample = false;
                }
                System.out.println(" *** ROW: " + lastRowOfCurrentCodeTable + " ***\033[0m");
            }
        } catch (NoSuchElementException e) {
            System.out.println("Done.");
        }
    }


    private final ItemsetStreamReader stream;
    private final int blockSize;
    private final int minFrequency;
    private final double maxImprovementRate;
    private final double minCodeTableDifference;
    private final int numSamples;
    private final double nSigma;

    private double sampleLengths[];
    private double minLength;
    private double maxLength;

    // Remember last code table and the corresponding head of stream found by `findCodeTable`.
    private CodeTable convergedCodeTable;
    private ImmutableList<ImmutableSet<String>> convergedHead;

    private void findCodeTable() {
        int sliceSize = blockSize;

        convergedHead = stream.head(sliceSize);
        convergedCodeTable = CodeTable.optimalFor(convergedHead, stream.items(), minFrequency);

        ImmutableList<ImmutableSet<String>> newHead;
        CodeTable newCodeTable;
        double ir, len, newLen;
        do {
            sliceSize += blockSize;
            newHead = stream.head(sliceSize);
            newCodeTable = CodeTable.optimalFor(newHead, stream.items(), minFrequency);

            len = convergedCodeTable.totalLengthOf(convergedHead);
            newLen = newCodeTable.totalLengthOf(convergedHead);
            ir = Math.abs(len - newLen) / len;

            convergedHead = newHead;
            convergedCodeTable = newCodeTable;
        } while (ir > maxImprovementRate);
    }

    private void discardBlocksConformingTo(boolean doSample) {
        // Sample head and calculate their encoded length.
        if (doSample) {
            sampleMeanLength();
        }

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

    private void sampleMeanLength() {

        Random random = new Random(System.currentTimeMillis());

        int sampleSize = convergedHead.size() < blockSize ? convergedHead.size() : blockSize;
        Set<Integer> indexes = new HashSet<>(sampleSize);

        double mean = 0;
        for (int i = 0; i < numSamples; i++) {
            indexes.clear();
            while (indexes.size() < sampleSize) {
                indexes.add(random.nextInt(convergedHead.size()));
            }

            ImmutableList.Builder<ImmutableSet<String>> sampleBuilder = ImmutableList.builder();
            for (int index : indexes) {
                sampleBuilder.add(convergedHead.get(index));
            }
            sampleLengths[i] += convergedCodeTable.totalLengthOf(sampleBuilder.build());
            mean += sampleLengths[i];
        }
        mean /= numSamples;

        double x, sigma = 0;
        for (int i = 0; i < numSamples; i++) {
            x = sampleLengths[i] - mean;
            sigma += x * x;
        }
        sigma /= numSamples;
        sigma = Math.sqrt(sigma);

        minLength = mean - nSigma * sigma;
        maxLength = mean - nSigma + sigma;
    }

}
