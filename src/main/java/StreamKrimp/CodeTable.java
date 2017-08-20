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

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;


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
        // These three lists are used as to get results from `compressedLengthFor`.
        List<ImmutableSet> itemsets = new ArrayList<>();
        List<ImmutableSet> candidates = new ArrayList<>();

        findCandidatesFor(streamSlice, minSupport, itemsets, candidates);

        List<Float> codeLengths = new ArrayList<>(candidates.size());
        List<Float> candidateCodeLength = new ArrayList<>(candidates.size());

        double currentLength = compressedLengthFor(streamSlice, itemsets, codeLengths);
        double lengthWithItemset;

        List<Float> temp;
        int insertionIndex = 0;
        for (ImmutableSet itemset : candidates) {
            // TODO[2]: Use the fastest data structure for frequent insertion and deletion in the middle.
            itemsets.add(insertionIndex, itemset);
            lengthWithItemset = compressedLengthFor(streamSlice, itemsets, candidateCodeLength);

            if (lengthWithItemset <= currentLength) {
                // Remove the itemset as it doesn't seem to contribute to the compression.
                itemsets.remove(insertionIndex);
            } else {
                // Store code lengths, but also use already allocated space for future computations.
                temp = codeLengths;
                codeLengths = candidateCodeLength;
                candidateCodeLength = temp;
                insertionIndex++;
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
                                          double minSupport,
                                          List<ImmutableSet> singletons,
                                          List<ImmutableSet> candidates) {
        // TODO[2]: Rewrite this method without using Spark.
        // Create Spark session.
        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        session.sparkContext().setLogLevel("OFF");

        Map<ImmutableSet, Integer> itemsMap = new HashMap<>();

        // Convert Immutable list of sets to list of Spark rows.
        List<Row> data = new ArrayList<>();
        for (ImmutableSet set : streamSlice) {
            for (Object item : set) {
                ImmutableSet<String> i = new ImmutableSet.Builder<String>().add((String) item).build();
                itemsMap.put(i, itemsMap.getOrDefault(i, 0) + 1);
            }
            data.add(RowFactory.create(set.asList()));
        }

        // Create schema for data frame.
        StructType schema = new StructType(new StructField[]{ new StructField(
                "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });

        // Create a data frame for sets of items.
        Dataset<Row> setsDataFrame = session.createDataFrame(data, schema);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(minSupport)
                .fit(setsDataFrame);

        List<Row> closedFrequents = model.freqItemsets().collectAsList();

        closedFrequents.sort((Row x, Row y) -> {
            int xSize = x.getList(0).size();
            int ySize = y.getList(0).size();

            if (xSize == ySize) {
                return (int)(y.getLong(1) - x.getLong(1));
            } else {
                return ySize - xSize;
            }
        });

        // Set singletons.
        singletons.clear();
        singletons.addAll(itemsMap.keySet());
        singletons.sort((ImmutableSet x, ImmutableSet y) -> {
            int xFreq = itemsMap.getOrDefault(x, 0);
            int yFreq = itemsMap.getOrDefault(y, 0);
            return yFreq - xFreq;
        });

        // Set candidates.
        candidates.clear();
        for (Row row : closedFrequents) {
            if (row.getList(0).size() < 2) {
                break;
            }
            ImmutableSet.Builder<String> setBuilder = new ImmutableSet.Builder<>();
            for (Object item : row.getList(0)) {
                setBuilder.add((String) item);
            }
            candidates.add(setBuilder.build());
        }
    }

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
