/*
 *      StreamKrimp/FrequentItemsetMiner.java
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

import ca.pfv.spmf.algorithms.frequentpatterns.apriori_close.AlgoAprioriClose;


class FrequentItemsetMiner {
    public static void run(ImmutableList<ImmutableSet<String>> database,
                           List<String> items,
                           int minFrequency,
                           Map<ImmutableSet<String>, Integer> outputMap) {
        if (outputMap == null) {
            throw new IllegalArgumentException("Output arguments cannot be null.");
        }

        try {
            // Write input database to a file.
            PrintWriter writer = new PrintWriter(inputDatabaseFilePath, "UTF-8");
            for (ImmutableSet<String> transaction : database) {
                writer.println(String.join(" ", transaction.asList()));
            }
            writer.close();

            // Find closed-frequent itemsets and write them in a file.
            double minSupport = (double) minFrequency / database.size();
            AlgoAprioriClose apriori = new AlgoAprioriClose();
            apriori.runAlgorithm(minSupport, inputDatabaseFilePath, outputItemsetsFilePath);

            // Add closed-frequent itemsets to `outputMap`.
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(outputItemsetsFilePath)));
            String line;
            String[] itemsetAndFrequency;
            ImmutableSet.Builder<String> itemsetBuilder;
            ImmutableSet<String> itemset;
            int frequency;
            while ((line = reader.readLine()) != null) {
                itemsetAndFrequency = line.split("\\s+#SUP:\\s+");

                // Build itemset.
                itemsetBuilder = ImmutableSet.<String>builder();
                for (String item : itemsetAndFrequency[0].split("\\s+")) {
                    itemsetBuilder.add(item);
                }
                itemset = itemsetBuilder.build();

                // Parse frequency.
                frequency = Integer.parseInt(itemsetAndFrequency[1]);

                // Store itemset and frequencies.
                outputMap.put(itemset, frequency);
            }
            reader.close();
        } catch (IOException e) {
            // TODO: Inform caller that a problem occurred.
            System.out.println("Something went wrong. Unable to continue.");
            System.exit(1);
        }
    }


    private static final String inputDatabaseFilePath = "tmp/input_database";
    private static final String outputItemsetsFilePath = "tmp/output_itemsets";

    // Make the class non-instantiatable.
    private FrequentItemsetMiner() {};

}
