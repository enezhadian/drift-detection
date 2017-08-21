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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

import DataStreamReader.ItemsetStreamReader;
import ca.pfv.spmf.patterns.itemset_array_integers_with_count.Itemset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import ca.pfv.spmf.algorithms.frequentpatterns.apriori_close.AlgoAprioriClose;
import ca.pfv.spmf.patterns.itemset_array_integers_with_count.Itemsets;


/**
 * TODO[4]: Documentation.
 */
class CodeTable {

    /**
     * TODO[4]: Documentation.
     * @param streamSlice
     * @return
     */
    public static CodeTable optimalFor(ImmutableList<ImmutableSet> streamSlice,
                                       ImmutableList items,
                                       double minSupport) {
        // These three lists are used as to get results from `findOptimalCodeLengthsFor`.
        List<ImmutableSet> itemsets = new ArrayList<>();
        List<ImmutableSet> candidates = new ArrayList<>();

        findCandidatesFor(streamSlice, items, minSupport, itemsets, candidates);
        System.out.println("Found " + itemsets.size() + " Singletons and "+ candidates.size() + " candidates.");

        List<Float> codeLengths = new ArrayList<>(candidates.size());
        List<Float> candidateCodeLengths = new ArrayList<>(candidates.size());

        double currentLength = findOptimalCodeLengthsFor(streamSlice, itemsets, codeLengths);
        double lengthWithItemset;

        List<Float> temp;
        int insertionIndex = 0;
        for (ImmutableSet itemset : candidates) {
            // TODO[2]: Use the fastest data structure for frequent insertion and deletion in the middle.
            itemsets.add(insertionIndex, itemset);
            lengthWithItemset = findOptimalCodeLengthsFor(streamSlice,
                    itemsets, candidateCodeLengths);

            if (lengthWithItemset > currentLength) {
                // Remove the itemset as it doesn't seem to contribute to the compression.
                itemsets.remove(insertionIndex);
            } else {
                // Store code lengths, but also use already allocated space for future computations.
                temp = codeLengths;
                codeLengths = candidateCodeLengths;
                candidateCodeLengths = temp;

                currentLength = lengthWithItemset;

                insertionIndex++;
            }
            // TODO[2]: Add pruning.
        }

        CodeTable codeTable = new CodeTable(
                ImmutableList.<ImmutableSet>builder().addAll(itemsets).build(),
                ImmutableList.<Float>builder().addAll(codeLengths).build());
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
    public double differenceWith(CodeTable that, ImmutableList<ImmutableSet> streamSlice) {
        // TODO[1]: Sometimes this is negative. Could it be an issue?
        double thisLen = this.totalLengthOf(streamSlice);
        double thatLen = that.totalLengthOf(streamSlice);
        return (thatLen - thisLen) / thisLen;
    }


    private static final double log2 = Math.log(2);

    private final ImmutableList<ImmutableSet> itemsets;
    private final ImmutableList<Float> codeLengths;

    /**
     * TODO[4]: Documentation.
     * @param streamSlice
     * @param minSupport
     * @param singletons
     * @param candidates
     */
    private static void findCandidatesFor(ImmutableList<ImmutableSet> streamSlice,
                                          ImmutableList items,
                                          double minSupport,
                                          List<ImmutableSet> singletons,
                                          List<ImmutableSet> candidates) {
        try {
            Files.delete(Paths.get("tempinfile"));
            Files.delete(Paths.get("tempoutfile"));

            PrintWriter writer = new PrintWriter("tempinfile", "UTF-8");
            for (ImmutableSet transaction : streamSlice) {
                writer.println(String.join(" ", transaction.asList()));
            }
            writer.close();

            AlgoAprioriClose apriori = new AlgoAprioriClose();
            apriori.runAlgorithm(minSupport, "tempinfile", "tempoutfile");

            candidates.clear();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("tempoutfile")));
            String line;
            String[] parts, is;
            Map<ImmutableSet, Integer> candidatesMap = new HashMap();
            while ((line = reader.readLine()) != null) {
                parts = line.split("\\s*#SUP:\\s*");
                is = parts[0].split("\\s*");

                if (is.length > 1) {
                    ImmutableSet.Builder setBuilder = ImmutableSet.builder();
                    for (Object item : is) {
                        setBuilder.add(item);
                    }
                    candidatesMap.put(setBuilder.build(), Integer.parseInt(parts[1]));
                }
            }
            candidates.addAll(candidatesMap.keySet());
            candidates.sort((ImmutableSet x, ImmutableSet y) -> {
                int xFreq = candidatesMap.get(x);
                int yFreq = candidatesMap.get(y);
                return yFreq - xFreq;
            });

            singletons.clear();
            for (Object item : items) {
                singletons.add(ImmutableSet.builder().add(item).build());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO[4]: Documentation.
     * @param streamSlice
     * @param itemsets Should be in proper order.
     * @param codeLengths
     * @return
     */
    private static double findOptimalCodeLengthsFor(ImmutableList<ImmutableSet> streamSlice,
                                                    List<ImmutableSet> itemsets,
                                                    List<Float> codeLengths) {
        // TODO[2]: Make this method faster.
        // Calculate the usage of each itemset and store them in `codeLengths`.
        codeLengths.clear();
        for (int i = 0; i < itemsets.size(); i++) {
            codeLengths.add(0f);
        }

        SetView residue;
        ImmutableSet itemset;
        for (ImmutableSet transaction : streamSlice) {
            // TODO[2]: Find a better way to create a `SetView` (Also applies to `coverLengthOf`).
            residue = Sets.intersection(transaction, transaction);

            for (int i = 0; i < itemsets.size(); i++) {
                itemset = itemsets.get(i);

                if (residue.containsAll(itemset)) {
                    codeLengths.set(i, codeLengths.get(i) + 1);

                    if (itemset.size() == transaction.size()) {
                        break;
                    } else {
                        residue = Sets.difference(residue, itemset);
                    }
                }
            }
        }

        // Calculate compressed length of stream slice.
        double totalUsage = 0;
        double compressedLength = 0; // Sum of code lengths of covers of transactions.
        for (float usage : codeLengths) {
            if (usage != 0) {
                totalUsage += usage;
                compressedLength += usage * (-1) * Math.log(usage) / log2;
            }
        }

        double logTotalUsage = Math.log(totalUsage) / log2;
        compressedLength += totalUsage * logTotalUsage;

        // Calculate the optimal code length for each itemset.
        double codeLength;
        for (int i = 0; i < codeLengths.size(); i++) {
            codeLength = codeLengths.get(i);
            if (codeLength != 0) {
                codeLengths.set(i, (float) (-Math.log(codeLength) / log2 + logTotalUsage));
            }
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
        // TODO[1]: Calculate encoded size of this code table.
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
        SetView residue = Sets.intersection(transaction, transaction);
        ImmutableSet itemset;
        double coverLength = 0;

        for (int i = 0; i < itemsets.size(); i++) {
            itemset = itemsets.get(i);

            if (residue.containsAll(itemset)) {
                coverLength += codeLengths.get(i);
                if (itemset.size() == transaction.size()) {
                    break;
                } else {
                    residue = Sets.difference(residue, itemset);
                }
            }
        }

        return coverLength;
    }

}
