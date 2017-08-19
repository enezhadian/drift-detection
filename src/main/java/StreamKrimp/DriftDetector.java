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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import DataStreamReader.ItemsetStreamReader;


public class DriftDetector {

    /**
     * TODO[4]: Documentation
     * @param stream
     * @param blockSize
     * @param minSupport
     * @param maxImprovementRate
     * @param minCodeTableDifference
     * @param leaveOut
     */
    public DriftDetector(ItemsetStreamReader stream, int blockSize, double minSupport,
                         double maxImprovementRate, double minCodeTableDifference,
                         double leaveOut) {

        this.stream = stream;
        this.blockSize = blockSize;
        this.minSupport = minSupport;
        this.maxImprovementRate = maxImprovementRate;
        this.minCodeTableDifference = minCodeTableDifference;
        this.leaveOut = leaveOut;
    }

    /**
     * TODO[4]: Documentation.
     */
    public void run() {
        // TODO[1]: Implement StreamKrimp algorithm.
        // Create a code table for the first block of data stream.
        CodeTableAndStreamSlice tableAndSlice;
        CodeTable currentCodeTable, candidateCodeTable;
        ImmutableList<ImmutableSet> head;

        tableAndSlice = findCodeTable();
        currentCodeTable = tableAndSlice.codeTable;

        while (true) {
            discardBlocksConformingTo(currentCodeTable);

            tableAndSlice = findCodeTable();
            candidateCodeTable = tableAndSlice.codeTable;
            head = tableAndSlice.streamSlice;

            // TODO[1]: candidate head here!
            double difference = candidateCodeTable.differenceWith(currentCodeTable, head);
            if (minCodeTableDifference <= difference) {
                // TODO[1]: Report concept drift.
                currentCodeTable = candidateCodeTable;
            } else {
                stream.discard(blockSize);
            }
        }
    }


    private final ItemsetStreamReader stream;
    private final int blockSize;
    private final double minSupport;
    private final double maxImprovementRate;
    private final double minCodeTableDifference;
    private final double leaveOut;

    private final class CodeTableAndStreamSlice {
        public CodeTable codeTable;
        public ImmutableList<ImmutableSet> streamSlice;
    }

    /**
     * TODO[4]: Documentation.
     * @return
     */
    private CodeTableAndStreamSlice findCodeTable() {
        // TODO[1]: Implement FIND CODE TABLE ON STREAM algorithm.
        return null;
    }

    /**
     * TODO[4]: Documentation.
     * @param codeTable
     * @return
     */
    private void discardBlocksConformingTo(CodeTable codeTable) {
        // TODO[1]: Find the next batch which does not seem to belong to the given code table.
    }

}
