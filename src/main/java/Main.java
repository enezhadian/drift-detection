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

import DataStreamReader.*;


public class Main {

    public static void runCDDA() throws Exception {
        final int blockSize = 10000;
        final double minChangeDegree = 0.1;

        CategoricalRecordStreamReader stream = new CategoricalRecordStreamReader("data/kddcup_10_percent.txt", false);
        new CDDA.DriftDetector(stream, blockSize, minChangeDegree).run();
    }

    public static void runCDCStream() throws Exception {
        final int blockSize = 10000;
        final double driftCoefficient = 3;

        CategoricalRecordStreamReader stream = new CategoricalRecordStreamReader("data/kddcup_10_percent.txt", false);
        new CDCStream.DriftDetector(stream, blockSize, driftCoefficient).run();
    }

    public static void main(String[] args) throws Exception {
        //runCDDA();
        runCDCStream();
    }

}
