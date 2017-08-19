/*
 *      StreamKrimp/CodeTable.java
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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


/**
 * TODO[4]: Documentation.
 */
class CodeTable {

    /**
     * TODO[4]: Documentation.
     * @param streamSlice
     * @return
     */
    public static CodeTable optimalFor(ImmutableList<ImmutableSet> streamSlice) {
        CodeTable codeTable = new CodeTable();
        // TODO[1]: Implement code-table construction algorithm.
        return codeTable;
    }

    /**
     * TODO[4]: Documentation.
     */
    private CodeTable() {
        entries = new ArrayList<>();
    }

    /**
     * TODO[4]: Documentation.
     * @param streamSlice
     * @return
     */
    public double totalLengthOf(ImmutableList<ImmutableSet> streamSlice) {
        return length() + lengthOf(streamSlice);
    }

    /**
     * TODO[4]: Documentation.
     * @param otherCodeTable
     * @param streamSlice
     * @return
     */
    public double differenceWith(CodeTable other, ImmutableList<ImmutableSet> streamSlice) {
        // TODO[1]: Calculate the code table difference for this and another code table.
        return 0;
    }


    // TODO: Implement java.io.Serializable
    private final class CodeTableEntry {
        ImmutableSet itemset;
        float codeLength;
    }

    private final List<CodeTableEntry> entries;

    /**
     * TODO[4]: Documentation.
     * @return
     */
    private double length() {
        // TODO: Calculate encoded size of this code table.
        return 0;
    }

    /**
     *  TODO[4]: Documentation.
     * @param streamSlice
     * @return
     */
    private double lengthOf(ImmutableList<ImmutableSet> streamSlice) {
        double totalCoverLength = 0;
        for (ImmutableSet transaction : streamSlice) {
            totalCoverLength = coverLengthOf(transaction);
        }
        return totalCoverLength;
    }

    /**
     * Calculate the length of encoded cover for a transaction.
     * @param transaction A transaction from data stream.
     * @return the sum of code lengths for all the itemsets in the cover of given transaction.
     */
    private double coverLengthOf(ImmutableSet transaction) {
        // TODO[1]: Calculate the cover size of a transaction using this code table.
        return 0;
    }

    /**
     * TODO[4]: Documentation.
     * @param other
     * @param streamSlice
     * @return
     */
    private double improvementRateOver(CodeTable other, ImmutableList<ImmutableSet> streamSlice) {
        // TODO[1]: Calculate the improvement rate of this over another code tables.
        return 0;
    }

}
