/*
 *      Main.java
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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import DataStreamReader.ItemsetStreamReader;
import StreamKrimp.CodeTable;
import StreamKrimp.DriftDetector;


public class Main {

    static final String inputFilePath = "data/letrecog.txt";
    static final String delimiterRegex = "\\s";
    static final int numItems = 102;
    static final List<String> items = items(0, numItems);
    static final int blockSize = numItems;
    static final int minFreq = 20;
    static final double maxIR = 0.02;
    static final double minCTD = 0.1;
    static final int numSamples = 100;
    static final double leaveOut = 0.01;

    static List<String> items(int incStart, int excEnd) {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < numItems; i++) {
            items.add(Integer.toString(i));
        }
        return items;
    }

    static void driftDetection() throws Exception {
        ItemsetStreamReader stream =
                new ItemsetStreamReader(inputFilePath, delimiterRegex, items);
        new DriftDetector(stream, blockSize, minFreq, maxIR, minCTD, numSamples, leaveOut).run();
        System.out.println("Processed " + stream.read + " transactions.");
    }

    public static void compress() throws Exception {
        ItemsetStreamReader stream =
                new ItemsetStreamReader(inputFilePath, delimiterRegex, items(0, numItems));
        ImmutableList<ImmutableSet<String>> head = stream.head(20000);
        CodeTable codeTable = CodeTable.optimalFor(head, items, minFreq);
    }

    public static void main(String[] args) throws Exception {
        // driftDetection();
        compress();
    }

}
