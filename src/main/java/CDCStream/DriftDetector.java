/*
 *      CDCStream/DriftDetector.java
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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;

import DataStreamReader.CategoricalRecordStreamReader;


public class DriftDetector {

    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    public DriftDetector(CategoricalRecordStreamReader stream,
                         int blockSize,
                         double driftCoefficient) {
        this.stream = stream;
        this.blockSize = blockSize;
        this.driftCoefficient = driftCoefficient;
    }

    public void run() {
        double blockSummary;
        double absoluteDifference, threshold;
        double mean = 0;
        double standardDeviation = 0, maxStandardDeviation = Double.MIN_VALUE, minStandardDeviation = Double.MAX_VALUE;

        ImmutableList<ImmutableList<String>> block;
        List<Double> summaries = new ArrayList<>();

        int lastCount = 0, currentCount = 0;
        try {
            System.out.println("Found concepts:");

            for (int i = 0; i < 2; i++) {
                block = stream.head(blockSize);
                stream.discard(block.size());

                summaries.add(summaryOf(block));
            }

            while (true) {
                currentCount = stream.countSoFar();
                block = stream.head(blockSize);
                stream.discard(block.size());

                blockSummary = summaryOf(block);

                if (summaries.size() > 1) {
                    // Calculate the mean.
                    mean = 0;
                    for (double summary : summaries) {
                        mean += summary;
                    }
                    mean /= summaries.size();

                    // Calculate the standard deviation.
                    standardDeviation = 0;
                    for (double summary : summaries) {
                        standardDeviation += Math.pow(summary - mean, 2);
                    }
                    standardDeviation = Math.sqrt(standardDeviation / summaries.size());

                    // Update `maxStandardDeviation` and `minStandardDeviation` if needed.
                    if (maxStandardDeviation < standardDeviation) {
                        maxStandardDeviation = standardDeviation;
                    }
                    if (minStandardDeviation > standardDeviation) {
                        minStandardDeviation = standardDeviation;
                    }
                } else if (summaries.size() == 1) {
                    mean = summaries.get(0);
                    standardDeviation = (minStandardDeviation + maxStandardDeviation) / 2;
                }

                absoluteDifference = Math.abs(blockSummary - mean);
                threshold = driftCoefficient * standardDeviation;
                if (absoluteDifference >= threshold) {
                    System.out.println(lastCount + "-" + currentCount);
                    lastCount = currentCount + 1;
                    summaries.clear();
                }

                summaries.add(blockSummary);
            }
        } catch (NoSuchElementException e) {
            if (currentCount >= lastCount) {
                System.out.println(lastCount + "-" + currentCount);
            }
            System.out.println("Done.");
        }
    }

    private final CategoricalRecordStreamReader stream;
    private final int blockSize;
    private final double driftCoefficient;

    private double summaryOf(ImmutableList<ImmutableList<String>> block) {
        if (block.size() == 0) {
            throw new IllegalArgumentException("Block should not be empty.");
        }

        DatabaseStatistics statistics = new DatabaseStatistics(block);
        int numAttributes = statistics.numAttributes();

        double summary = 0;
        for (int attribute = 0; attribute < numAttributes; attribute++) {
            summary += DILCA.distanceMatrixFor(statistics, attribute).normalizedSquaredSumRoot();
            // System.out.print(".");
            // System.out.flush();
        }
        summary /= numAttributes;
        // System.out.println();

        return summary;
    }

}
