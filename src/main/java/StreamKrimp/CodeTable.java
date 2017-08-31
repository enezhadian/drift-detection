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

import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;


// FIXME: Length names are messy. Come up with a uniform naming for them.
public class CodeTable {

    /*--------------------------------------------------------------------------*
     *                        STATIC MEMBERS AND METHODS                        *
     *--------------------------------------------------------------------------*/

    public static CodeTable optimalFor(ImmutableList<ImmutableSet<String>> streamSlice,
                                       List<String> items,
                                       double minSupport) {
//                                       int minFrequency) {
        // TODO: Tidy up this method.
        // These three lists are used as to get results from `findOptimalCodeLengthsFor`.
        List<ImmutableSet<String>> itemsets = new ArrayList<>();
        List<ImmutableSet<String>> candidates = new ArrayList<>();
        List<ImmutableSet<String>> singletons = new ArrayList<>();

//        findCandidatesFor(streamSlice, items, minFrequency, singletons, candidates);
        findCandidatesFor(streamSlice, items, minSupport, singletons, candidates);
        System.out.println("Found " + singletons.size() + " Singletons and " + candidates.size() + " candidates.");

        List<Integer> usageCounts = new ArrayList<>(candidates.size());
        List<Integer> candidateUsageCounts = new ArrayList<>(candidates.size());
        List<Float> codeLengths = new ArrayList<>(candidates.size());
        List<Float> candidateCodeLengths = new ArrayList<>(candidates.size());

        double currentLength, lengthWithItemset;

        itemsets.addAll(singletons);
        findUsageCountsFor(streamSlice, itemsets, usageCounts);
        currentLength = calculateOptimalCodeLengthsFor(usageCounts, codeLengths);

        CodeTable standardCodeTable = new CodeTable(
                ImmutableList.<ImmutableSet<String>>builder().addAll(itemsets).build(),
                ImmutableList.<Float>builder().addAll(codeLengths).build(),
                0);

        List temp;
        int insertionIndex = 0;
        for (ImmutableSet<String> itemset : candidates) {
            itemsets.add(insertionIndex, itemset);

            // copy previous usage counts to `candidateUsageCounts`.
            candidateUsageCounts.clear();
            candidateUsageCounts.addAll(usageCounts);
            candidateUsageCounts.add(insertionIndex, 0);

            updateUsageCountsFor(streamSlice, itemsets, insertionIndex, candidateUsageCounts);
            lengthWithItemset = calculateOptimalCodeLengthsFor(candidateUsageCounts, candidateCodeLengths);

            // FIXME: This does not take code table length into account.
            if (lengthWithItemset >= currentLength) {
                // Remove the itemset as it doesn't seem to contribute to the compression.
                itemsets.remove(insertionIndex);

            } else {
                // Store usage counts, but also use already allocated space for future computations.
                temp = usageCounts;
                usageCounts = candidateUsageCounts;
                candidateUsageCounts = temp;

                // Store code lengths, but also use already allocated space for future computations.
                temp = codeLengths;
                codeLengths = candidateCodeLengths;
                candidateCodeLengths = temp;

                currentLength = lengthWithItemset;

                insertionIndex++;
            }
            // TODO: Add pruning.
        }

        ImmutableList<ImmutableSet<String>> codeTableItemsets =
                ImmutableList.<ImmutableSet<String>>builder().addAll(itemsets).build();
        ImmutableList<Float> codeTableCodeLengths = ImmutableList.<Float>builder().addAll(codeLengths).build();
        double length = 0;
        for (int i = 0; i < codeTableItemsets.size(); i++) {
            length += standardCodeTable.coverLengthOf(codeTableItemsets.get(i));
            length += codeTableCodeLengths.get(i);
        }
        System.out.println("Best length found: " + currentLength);

        CodeTable codeTable = new CodeTable(codeTableItemsets, codeTableCodeLengths, length);
        return codeTable;
    }

    private static final double log2 = Math.log(2);

    private static void findCandidatesFor(ImmutableList<ImmutableSet<String>> streamSlice,
                                          List<String> items,
//                                          int minFrequency,
                                          double minSupport,
                                          List<ImmutableSet<String>> outputSingletons,
                                          List<ImmutableSet<String>> outputCandidates) {
        if (outputSingletons == null || outputCandidates == null) {
            throw new IllegalArgumentException("Output arguments cannot be null.");
        }

        Map<ImmutableSet<String>, Integer> itemsetsWithFrequencies = new HashMap<>();
//        FrequentItemsetMiner2.run(streamSlice, items, minFrequency, itemsetsWithFrequencies);
        FrequentItemsetMiner2.run(streamSlice, items, minSupport, itemsetsWithFrequencies);

        // Populate `outputSingletons` with singletons.
        outputSingletons.clear();
        for (String item : items) {
            outputSingletons.add(ImmutableSet.<String>builder().add(item).build());
        }
        // Sort `outputSingletons`. TODO: This is actually not needed.
        outputSingletons.sort((ImmutableSet<String> x, ImmutableSet<String> y) -> {
            int xFreq = itemsetsWithFrequencies.getOrDefault(x, 0);
            int yFreq = itemsetsWithFrequencies.getOrDefault(y, 0);
            return yFreq - xFreq;
        });

        // Populate `outputCandidates` with non-singleton frequent itemsets.
        outputCandidates.clear();
        for (ImmutableSet<String> itemset : itemsetsWithFrequencies.keySet()) {
            if (itemset.size() > 1) {
                outputCandidates.add(itemset);
            }
        }
        // Sort `outputCandidates`.
        outputCandidates.sort((ImmutableSet<String> x, ImmutableSet<String> y) -> {
            int xSize = x.size();
            int ySize = y.size();
            if (xSize == ySize) {
                int xFreq = itemsetsWithFrequencies.get(x);
                int yFreq = itemsetsWithFrequencies.get(y);
                return yFreq - xFreq;
            } else {
                return ySize - xSize;
            }
        });
    }

    private static void findUsageCountsFor(ImmutableList<ImmutableSet<String>> streamSlice,
                                           List<ImmutableSet<String>> itemsets,
                                           List<Integer> outputUsageCounts) {
        if (outputUsageCounts == null) {
            throw new IllegalArgumentException("Output arguments cannot be null.");
        }

        outputUsageCounts.clear();
        for (int i = 0; i < itemsets.size(); i++) {
            outputUsageCounts.add(0);
        }

        for (ImmutableSet<String> transaction : streamSlice) {
            // TODO: Find a better way to create a `SetView`.
            cover(transaction, itemsets, outputUsageCounts);
        }
    }

    private static void updateUsageCountsFor(ImmutableList<ImmutableSet<String>> streamSlice,
                                             List<ImmutableSet<String>> itemsets,
                                             int newItemsetIndex,
                                             List<Integer> inputOutputUsageCounts) {
        ImmutableSet<String> newItemset = itemsets.get(newItemsetIndex);

        SetView<String> residue;
        ImmutableSet<String> itemset;
        for (ImmutableSet<String> transaction : streamSlice) {
            if (transaction.containsAll(newItemset)) {
                // Uncover previous usages for this transaction.
                residue = Sets.intersection(transaction, transaction);

                for (int i = 0; i < itemsets.size(); i++) {
                    if (i != newItemsetIndex) {
                        itemset = itemsets.get(i);

                        if (residue.containsAll(itemset)) {
                            inputOutputUsageCounts.set(i, inputOutputUsageCounts.get(i) - 1);

                            if (itemset.size() == residue.size()) {
                                break;
                            } else {
                                residue = Sets.difference(residue, itemset);
                            }
                        }
                    }
                }

                // Cover the transaction with new itemsets.
                cover(transaction, itemsets, inputOutputUsageCounts);
            }
        }
    }

    private static void cover(ImmutableSet<String> transaction,
                              List<ImmutableSet<String>> itemsets,
                              List<Integer> inputOutputUsageCounts) {
        // TODO: Find a better way to create a `SetView`.
        SetView<String> residue = Sets.intersection(transaction, transaction);
        ImmutableSet<String> itemset;

        for (int i = 0; i < itemsets.size(); i++) {
            itemset = itemsets.get(i);

            if (residue.containsAll(itemset)) {
                inputOutputUsageCounts.set(i, inputOutputUsageCounts.get(i) + 1);

                if (itemset.size() == residue.size()) {
                    break;
                } else {
                    residue = Sets.difference(residue, itemset);
                }
            }
        }
    }

    private static double calculateOptimalCodeLengthsFor(List<Integer> usageCounts,
                                                         List<Float> outputCodeLength) {
        if (outputCodeLength == null) {
            throw new IllegalArgumentException("Output arguments cannot be null.");
        }

        // Calculate total usage count.
        double totalUsage = 0;
        for (double usage : usageCounts) {
            totalUsage += usage;
        }

        // Calculate optimal code lengths and total cover size.
        float codeLength;
        double sliceLength = 0;

        outputCodeLength.clear();
        for (double usage : usageCounts) {
            if (usage > 0) {
                codeLength = (float)(-Math.log(usage / totalUsage) / log2);
                sliceLength += usage * codeLength;
            } else {
                codeLength = 0;
            }
            outputCodeLength.add(codeLength);
        }

        return sliceLength;
    }

    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    public double totalLengthOf(ImmutableList<ImmutableSet<String>> streamSlice) {
        // TODO: Revert this.
//        return length() + lengthOf(streamSlice);
        return lengthOf(streamSlice);
    }

    private final ImmutableList<ImmutableSet<String>> itemsets;
    private final ImmutableList<Float> codeLengths;
    private final double length;

    private CodeTable(ImmutableList<ImmutableSet<String>> itemsets,
                      ImmutableList<Float> codeLengths,
                      double length) {
        this.itemsets = itemsets;
        this.codeLengths = codeLengths;
        this.length = length;
    }

    private double length() {
        return length;
    }

    private double lengthOf(ImmutableList<ImmutableSet<String>> streamSlice) {
        double totalCoverLength = 0;
        for (ImmutableSet<String> transaction : streamSlice) {
            totalCoverLength += coverLengthOf(transaction);
        }
        return totalCoverLength;
    }

    private double coverLengthOf(ImmutableSet<String> transaction) {
        // TODO: Find a better way to create a `SetView`
        SetView<String> residue = Sets.intersection(transaction, transaction);
        ImmutableSet<String> itemset;
        double coverLength = 0;

        for (int i = 0; i < itemsets.size(); i++) {
            itemset = itemsets.get(i);

            if (residue.containsAll(itemset)) {
                coverLength += codeLengths.get(i);
                if (itemset.size() == residue.size()) {
                    break;
                } else {
                    residue = Sets.difference(residue, itemset);
                }
            }
        }

        return coverLength;
    }

}
