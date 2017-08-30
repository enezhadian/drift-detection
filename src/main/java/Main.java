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

import CDCStream.DILCA;
import CDCStream.DatabaseStatistics;
import CDCStream.MapDILCA;
import DataStreamReader.*;
import com.google.common.collect.ImmutableList;

import java.util.*;


public class Main {

    public static void runCDDA() throws Exception {
        final int blockSize = 10000;
        final double minChangeDegree = 0.1;

        CategoricalRecordStreamReader stream = new CategoricalRecordStreamReader("data/kddcup_10_percent.txt", false);
        new CDDA.DriftDetector(stream, blockSize, minChangeDegree).run();
    }

    public static void runCDCStream() throws Exception {
        final int blockSize = 1000;
        final double driftCoefficient = 3;

        CategoricalRecordStreamReader stream = new CategoricalRecordStreamReader("data/kddcup_10_percent.txt", false);
        new CDCStream.DriftDetector(stream, blockSize, driftCoefficient).run();
    }

    public static void compareDILCA() throws Exception {
        final int blockSize = 1000;
        final int targetAttributeIndex = 2;

        CategoricalRecordStreamReader stream = new CategoricalRecordStreamReader("data/kddcup_10_percent.txt", false);
        ImmutableList<ImmutableList<String>> block = stream.head(blockSize);

        DatabaseStatistics statistics = new DatabaseStatistics(block);
        DILCA dilca = DILCA.distanceMatrixFor(statistics, targetAttributeIndex);

        MapDILCA mapDilca = MapDILCA.distanceMatrixFor(block, targetAttributeIndex);

        Map<String, Integer> domain = statistics.attributeDomains.get(targetAttributeIndex);
        List<String> values = new ArrayList<>(domain.keySet());
        values.sort(Comparator.comparingInt((x) -> domain.get(x)));

        System.out.println(domain.size());

        System.out.println("============ DILCA ============");
        for (int i = 0; i < dilca.distances.length; i++) {
            for (int j = i + 1; j < dilca.distances[i].length; j++) {
                System.out.printf("%.2f ", dilca.distances[i][j]);
            }
            System.out.println();
        }

        System.out.println("============ MapDILCA ============");
        for (String value1 : values) {
            for (String value2 : values) {
                System.out.printf("%.2f ", mapDilca.distances.get(value1).get(value2));
            }
            System.out.println();
        }

        System.out.println(dilca.normalizedSquaredSumRoot() == mapDilca.normalizedSquaredSumRoot());
    }

    public static void statistics() throws Exception {
        final int blockSize = 1000;
        final int targetAttributeIndex = 2;

        CategoricalRecordStreamReader stream = new CategoricalRecordStreamReader("data/test.txt", false);
        ImmutableList<ImmutableList<String>> block = stream.head(blockSize);

        DatabaseStatistics statistics = new DatabaseStatistics(block);

        int n = statistics.numAttributes();
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                System.out.println("======================================");
                Map<String, Integer> D1 = statistics.attributeDomains.get(i);
                Map<String, Integer> D2 = statistics.attributeDomains.get(j);

                List<String> vs1 = new ArrayList<>(D1.keySet());
                vs1.sort(Comparator.comparingInt(x -> D1.get(x)));
                for (String v : vs1) { System.out.printf("%s(%d)    ", v, D1.get(v)); }
                System.out.println();

                List<String> vs2 = new ArrayList<>(D2.keySet());
                vs2.sort(Comparator.comparingInt(x -> D2.get(x)));
                for (String v : vs2) { System.out.printf("%s(%d)    ", v, D2.get(v)); }
                System.out.println();

                for (int k = 0; k < D1.size(); k++) {
                    for (int l = 0; l < D2.size(); l++) {
                        System.out.printf("%d ", statistics.cooccurrences[i][j][k][l]);
                    }
                    System.out.println();
                }
            }
        }
        DILCA d = DILCA.distanceMatrixFor(statistics, 0);
        System.out.println("Done");
    }

    public static void main(String[] args) throws Exception {
        // runCDDA();
        // runCDCStream();
        // compareDILCA();
        statistics();
    }

}
