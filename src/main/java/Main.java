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
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import DataStreamReader.ItemsetStreamReader;
import StreamKrimp.DriftDetector;



public class Main {

    public static void main(String[] args) throws Exception {
        List<String> items = new ArrayList<>();
        for (int i = 0; i < 58; i++) {
            items.add(Integer.toString(i));
        }

        ItemsetStreamReader stream = new ItemsetStreamReader(
                "data/chessBig.txt", "\\s", items);

        ImmutableSet first = stream.head(10).get(0);
        for (Object item : first) {
            System.out.println(item.getClass() + " " + item);
        }

        DriftDetector detector = new DriftDetector(stream, 58, 0.1, 0.02, 0.1, 10, 0.01);

        detector.run();
    }

}
