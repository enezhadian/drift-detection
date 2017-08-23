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
import java.util.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import ca.pfv.spmf.algorithms.frequentpatterns.apriori_close.AlgoAprioriClose;


public class CodeTable {

    public static CodeTable optimalFor(ImmutableList<ImmutableSet<String>> streamSlice,
                                       List<String> items,
                                       int minFrequency) {
        // These three lists are used as to get results from `findOptimalCodeLengthsFor`.
        List<ImmutableSet<String>> itemsets = new ArrayList<>();
        List<ImmutableSet<String>> candidates = new ArrayList<>();
        List<ImmutableSet<String>> singletons = new ArrayList<>();

        findCandidatesFor(streamSlice, items, minFrequency, singletons, candidates);
        System.out.println("Found " + singletons.size() + " Singletons and "
                + candidates.size() + " candidates.");

        itemsets.addAll(singletons);
        List<Float> codeLengths = new ArrayList<>(candidates.size());
        List<Float> candidateCodeLengths = new ArrayList<>(candidates.size());

        double currentLength = findOptimalCodeLengthsFor(streamSlice, itemsets, codeLengths);
        CodeTable standardCodeTable = new CodeTable(
                ImmutableList.<ImmutableSet<String>>builder().addAll(itemsets).build(),
                ImmutableList.<Float>builder().addAll(codeLengths).build(),
                0);

        double lengthWithItemset;

        List<Float> temp;
        int insertionIndex = 0;
        for (ImmutableSet<String> itemset : candidates) {
            // TODO: Use the fastest data structure for frequent insertion and deletion in the middle.
            itemsets.add(insertionIndex, itemset);

            // System.out.println("======================================");
            // for (ImmutableSet<String> is : itemsets) {
            //     System.out.println(String.join(" ", is));
            // }

            lengthWithItemset = findOptimalCodeLengthsFor(streamSlice,
                    itemsets, candidateCodeLengths);

            // System.out.print("Length with itemset " + Arrays.toString(itemset.toArray()) + ": " +
            //         lengthWithItemset);

            if (lengthWithItemset == currentLength) {
                // System.out.println("=> === EQUAL ===");
                // Remove the itemset as it doesn't seem to contribute to the compression.
                itemsets.remove(insertionIndex);
            }
            else if (lengthWithItemset > currentLength) {
                // System.out.println("=> --- IGNORED ---");
                // Remove the itemset as it doesn't seem to contribute to the compression.
                itemsets.remove(insertionIndex);

            } else {
                // System.out.println("=> +++ ADDED +++");
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
        ImmutableList<Float> codeTableCodeLengths =
                ImmutableList.<Float>builder().addAll(codeLengths).build();
        double length = 0;
        for (int i = 0; i < codeTableItemsets.size(); i++) {
            length += standardCodeTable.coverLengthOf(codeTableItemsets.get(i));
            length += codeTableCodeLengths.get(i);
        }
        System.out.println("Best length found: " + currentLength);

        CodeTable codeTable = new CodeTable(codeTableItemsets, codeTableCodeLengths, length);
        return codeTable;
    }

    public double totalLengthOf(ImmutableList<ImmutableSet<String>> streamSlice) {
        return length() + lengthOf(streamSlice);
    }


    private static final double log2 = Math.log(2);

    private final ImmutableList<ImmutableSet<String>> itemsets;
    private final ImmutableList<Float> codeLengths;
    private final double length;

    private static void findCandidatesFor(ImmutableList<ImmutableSet<String>> streamSlice,
                                          List<String> items,
                                          int minFrequency,
                                          List<ImmutableSet<String>> outputSingletons,
                                          List<ImmutableSet<String>> outputCandidates) {
        String input = "tmp/input";
        String output = "tmp/output";
        try {
            PrintWriter writer = new PrintWriter(input, "UTF-8");
            for (ImmutableSet<String> transaction : streamSlice) {
                writer.println(String.join(" ", transaction.asList()));
            }
            writer.close();

            double minSupport = (double) minFrequency / streamSlice.size();
            AlgoAprioriClose apriori = new AlgoAprioriClose();
            apriori.runAlgorithm(minSupport, input, output);

            outputCandidates.clear();
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(output)));
            String line;
            String[] parts, is;
            Map<ImmutableSet<String>, Integer> frequenciesMap = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                parts = line.split("\\s+#SUP:\\s+");
                is = parts[0].split("\\s+");

                if (is.length > 1) {
                    ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();
                    for (String item : is) {
                        setBuilder.add(item);
                    }
                    frequenciesMap.put(setBuilder.build(), Integer.parseInt(parts[1]));
                } else {
                    frequenciesMap.put(ImmutableSet.<String>builder().add(is[0]).build(),
                            Integer.parseInt(parts[1]));
                }
            }
            outputCandidates.addAll(frequenciesMap.keySet());
            outputCandidates.sort((ImmutableSet<String> x, ImmutableSet<String> y) -> {
                int xSize = x.size();
                int ySize = y.size();
                if (xSize == ySize) {
                    int xFreq = frequenciesMap.get(x);
                    int yFreq = frequenciesMap.get(y);
                    return yFreq - xFreq;
                } else {
                    return ySize - xSize;
                }
            });

            outputSingletons.clear();
            for (String item : items) {
                outputSingletons.add(ImmutableSet.<String>builder().add(item).build());
            }
            // This is actually not needed.
            outputSingletons.sort((ImmutableSet<String> x, ImmutableSet<String> y) -> {
                int xFreq = frequenciesMap.getOrDefault(x, 0);
                int yFreq = frequenciesMap.getOrDefault(y, 0);
                return yFreq - xFreq;
            });

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static double findOptimalCodeLengthsFor(ImmutableList<ImmutableSet<String>> streamSlice,
                                                    List<ImmutableSet<String>> itemsets,
                                                    List<Float> outputCodeLengths) {
        // TODO: Make this method faster.
        // Calculate the usage of each itemset and store them in `codeLengths`.
        if (outputCodeLengths == null) {
            outputCodeLengths = new ArrayList<>(itemsets.size());
        }

        outputCodeLengths.clear();
        for (int i = 0; i < itemsets.size(); i++) {
            outputCodeLengths.add(0f);
        }

        SetView<String> residue;
        ImmutableSet<String> itemset;
        for (ImmutableSet<String> transaction : streamSlice) {
            // TODO: Find a better way to create a `SetView` (Also applies to `coverLengthOf`).
            residue = Sets.intersection(transaction, transaction);

            for (int i = 0; i < itemsets.size(); i++) {
                itemset = itemsets.get(i);

                if (residue.containsAll(itemset)) {
                    outputCodeLengths.set(i, outputCodeLengths.get(i) + 1);

                    if (itemset.size() == residue.size()) {
                        break;
                    } else {
                        residue = Sets.difference(residue, itemset);
                    }
                }
            }
        }

        // Calculate compressed length of stream slice.
        float totalUsage = 0;
        for (float usage : outputCodeLengths) {
            totalUsage += usage;
        }

        float usage, codeLength;
        double compressedLength = 0; // Sum of code lengths of covers of transactions.
        for (int i = 0; i < outputCodeLengths.size(); i++) {
            usage = outputCodeLengths.get(i);
            if (usage > 0) {
                codeLength = (float)(-Math.log(usage / totalUsage) / log2);
                compressedLength += usage * codeLength;
                outputCodeLengths.set(i, codeLength);
            }
        }

        return compressedLength;
    }

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
