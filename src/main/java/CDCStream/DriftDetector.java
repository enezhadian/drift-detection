/*
 *      CDCStream/DriftDetector.java
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
g
package CDCStream;

import java.util.NoSuchElementException;
import java.util.Set;

import DataStreamReader.CategoricalRecordStreamReader;
import com.google.common.collect.ImmutableList;


public class DriftDetector {

    public DriftDetector(CategoricalRecordStreamReader stream, int blockSize) {}

    public void run() {
        try {
            while (true) {
                // TODO: Implement this.
            }
        } catch (NoSuchElementException e) {
            System.out.println("Done.");
        }
    }

    private double summaryOf(ImmutableList<ImmutableList<String>> block) {
        if (block.size() == 0) {
            throw new IllegalArgumentException("Block should not be empty.");
        }

        int numAttributes = block.get(0).size();

        double summary = 0;
        for (int attribute = 0; attribute < numAttributes; attribute++) {
            summary += DILCA.distanceMatrixFor(block, attribute).normalizedSquaredSum();
        }
        summary /= numAttributes;

        return summary;
    }

}
