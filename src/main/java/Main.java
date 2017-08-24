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

import DataStreamReader.*;


public class Main {

    public static void runCDDA() throws Exception {
        final int blockSize = 10000;
        final double minChangeDegree = 0.1;

        TabularStreamReader stream = new TabularStreamReader("data/kddcup_10_percent.txt", false);
        new CDDA.DriftDetector(stream, blockSize, minChangeDegree).run();
    }

    static void runStreamKrimp() throws Exception {
        final String inputFilePath = "data/wine.txt";
        final String delimiterRegex = "\\s";
        final int numItems = 68;
        final int blockSize = numItems;
        final int minFreq = 5;
        final double maxIR = 0.02;
        final double minCTD = 0.1;
        final int numSamples = 100;
        final double nSigma = 2;

        final List<String> items = new ArrayList<>();
        for (int i = 0; i < numItems; i++) {
            items.add(Integer.toString(i));
        }

        ItemsetStreamReader stream =
                new ItemsetStreamReader(inputFilePath, delimiterRegex, items);
        new StreamKrimp.DriftDetector(stream, blockSize, minFreq, maxIR, minCTD, numSamples, nSigma).run();
        System.out.println("Processed " + stream.read + " transactions.");
    }

    public static void main(String[] args) throws Exception {
        // runCDDA();
        runStreamKrimp();
    }

}
