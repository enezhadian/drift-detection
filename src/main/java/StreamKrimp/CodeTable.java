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
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;


/**
 * TODO[4]: Documentation.
 */
class CodeTable {

    /**
     * TODO[4]: Documentation.
     * @param streamSlice
     * @return
     */
    public static CodeTable optimalFor(ImmutableList<ImmutableSet> streamSlice, double minSupport) {
        ClosedFrequentSetMiner miner = new ClosedFrequentSetMiner(streamSlice);

        // TODO[1]: All these lists should have the proper order (Increasing in length and support).
        ImmutableList<ImmutableSet> candidateItemsets = miner.nonSingletonClosedFrequentItemsets(
                minSupport);

        // These three lists are used as to get results from `compressedLengthFor`.
        List<ImmutableSet> itemsets = new ArrayList<>(miner.singletons());
        List<Float> codeLengths = new ArrayList<>(candidateItemsets.size());
        List<Float> candidateCodeLength = new ArrayList<>(candidateItemsets.size());


        double currentLength = compressedLengthFor(streamSlice, itemsets, codeLengths);
        double lengthWithItemset;

        List<Float> temp;
        for (int i = itemsets.size() - 1; i >= 0; i++)
        for (ImmutableSet itemset : candidateItemsets) {
            // TODO[1]: Itemset should be appended in the right place.
            itemsets.add(itemset);
            lengthWithItemset = compressedLengthFor(streamSlice, itemsets, candidateCodeLength);

            if (lengthWithItemset <= currentLength) {
                // Remove the itemset as it doesn't seem to contribute to the compression.
                itemsets.remove(itemsets.size() - 1);
            } else {
                // Store code lengths, but also use already allocated space for future computations.
                temp = codeLengths;
                codeLengths = candidateCodeLength;
                candidateCodeLength = temp;
            }
            // TODO[2]: Add pruning.
        }

        CodeTable codeTable = new CodeTable(
                new ImmutableList.Builder<ImmutableSet>().addAll(itemsets).build(),
                new ImmutableList.Builder<Float>().addAll(codeLengths).build());
        return codeTable;
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
        double l1 = this.totalLengthOf(streamSlice);
        double l2 = other.totalLengthOf(streamSlice);
        return (l2 - l1) / l1;
    }


    private final ImmutableList<ImmutableSet> itemsets;
    private final ImmutableList<Float> codeLengths;

    /**
     * TODO[4]: Documentation.
     * @param streamSlice
     * @param itemsets Should be in proper order.
     * @param codeLengths
     * @return
     */
    private static double compressedLengthFor(ImmutableList<ImmutableSet> streamSlice,
                                              List<ImmutableSet> itemsets,
                                              List<Float> codeLengths) {
        // Calculate the usage of each itemset and store them in `codeLengths`.
        codeLengths.clear();
        for (int i = 0; i < itemsets.size(); i++) {
            codeLengths.add(0f);
        }

        SetView residue;
        ImmutableSet itemset;
        for (ImmutableSet transaction : streamSlice) {
            // TODO[2]: Find a better way to create a `SetView`.
            residue = Sets.intersection(transaction, transaction);

            for (int i = 0; i < itemsets.size(); i++) {
                itemset = itemsets.get(i);

                if (residue.containsAll(itemset)) {
                    if (itemset.size() == transaction.size()) {
                        codeLengths.set(i, codeLengths.get(i) + 1);
                        break;
                    } else {
                        // Calculate difference.
                        residue = Sets.difference(residue, itemset);
                    }
                }
            }
        }

        // Calculate compressed length of stream slice.
        double totalUsage = 0;
        double compressedLength = 0; // Sum of code lengths of covers of transactions.
        double log2 = Math.log(2);
        for (float usage : codeLengths) {
            totalUsage += usage;
            compressedLength += usage * (-1) * Math.log(usage) / log2;
        }

        double logTotalUsage = Math.log(totalUsage) / log2;
        compressedLength += totalUsage * logTotalUsage;

        // Calculate the optimal code length for each itemset.
        for (int i = 0; i < codeLengths.size(); i++) {
            codeLengths.set(i, (float) (-Math.log(codeLengths.get(i)) / log2 + logTotalUsage));
        }

        return compressedLength;
    }

    /**
     * TODO[4]: Documentation.
     */
    private CodeTable(ImmutableList<ImmutableSet> itemsets, ImmutableList<Float> codeLengths) {
        this.itemsets = itemsets;
        this.codeLengths = codeLengths;
    }

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

}
