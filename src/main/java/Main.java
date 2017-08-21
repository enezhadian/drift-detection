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

import DataStreamReader.ItemsetStreamReader;
import StreamKrimp.DriftDetector;


public class Main {

    public static void main(String[] args) throws Exception {
        int numItems = 102;
        int blockSize = numItems;
        double minSupport = 0.3;
        double maxImprovementRate = 0.02;
        double minCodeTableDifference = 0.1;
        int numSamples = 100;
        double leaveOut = 0.01;

        List<String> items = new ArrayList<>();
        for (int i = 0; i < numItems; i++) {
            items.add(Integer.toString(i));
        }

        ItemsetStreamReader stream = new ItemsetStreamReader(
                "data/letrecog.txt", "\\s", items);

        DriftDetector detector = new DriftDetector(stream, numItems, minSupport,
                maxImprovementRate,minCodeTableDifference, numSamples, leaveOut);

        detector.run();

        System.out.println("Processed " + stream.read + " transactions.");
    }

}
