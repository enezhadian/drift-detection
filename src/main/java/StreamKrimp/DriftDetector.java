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
import org.aopalliance.reflect.Code;


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
        CodeTable currentCodeTable;

        // Create a code table for the first block of data stream.
        findCodeTable();
        currentCodeTable = candidateCodeTable;

        while (true) {
            discardBlocksConformingTo(currentCodeTable);

            findCodeTable();
            double difference = candidateCodeTable.differenceWith(
                    currentCodeTable, headForCandidateCodeTable);

            if (minCodeTableDifference <= difference) {
                // TODO[2]: Report concept drift.
                System.out.println("Concept drift detected.");
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

    // Remember last code table and the corresponding head of stream found by `findCodeTable`.
    private CodeTable candidateCodeTable;
    private ImmutableList<ImmutableSet> headForCandidateCodeTable;

    /**
     * TODO[4]: Documentation.
     * @return
     */
    private void findCodeTable() {
        candidateCodeTable = null;
        headForCandidateCodeTable = null;

        int sliceSize = blockSize;
        ImmutableList<ImmutableSet> head = stream.head(sliceSize);
        CodeTable codeTable = CodeTable.optimalFor(head, minSupport);
        CodeTable newCodeTable;

        double ir, len, newLen;
        do {
            sliceSize += blockSize;
            head = stream.head(sliceSize);
            newCodeTable = CodeTable.optimalFor(head, minSupport);

            len = codeTable.totalLengthOf(head);
            newLen = newCodeTable.totalLengthOf(head);
            ir = Math.abs(len - newLen) / len;

            codeTable = newCodeTable;
        } while (ir <= maxImprovementRate);

        candidateCodeTable = codeTable;
        headForCandidateCodeTable = head;
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
