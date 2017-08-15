/*
 *      DataStreamReader/SetStreamReader.java
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

package DataStreamReader;

import java.io.*;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


// TODO: Make this thread-safe to be able to respond to multiple stream processors.
// TODO: Use an integer rather than keeping multiple copies of each item's name. For this purpose use a builder pattern to use the smallest integer type which can cover all items.
public class SetStreamReader {

    public SetStreamReader(String path, String delimiterRegex) throws FileNotFoundException {
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        delimiter = Pattern.compile(delimiterRegex);
    }

    ImmutableList<ImmutableSet> nextBatch(int maxSize) {
        ImmutableList.Builder<ImmutableSet> batchBuilder = new ImmutableList.Builder<>();


        int size = 0;
        String line;
        try {
            while (size < maxSize && (line = reader.readLine()) != null) {
                size++;
                ImmutableSet.Builder<String> setBuilder = new ImmutableSet.Builder<>();

                for (String item : delimiter.split(line)) {
                    setBuilder.add(item);
                }

                batchBuilder.add(setBuilder.build());
            }
        } catch (IOException e) {
            // Simply continue by returning what already has been read.
        }

        return batchBuilder.build();
    }


    private final BufferedReader reader;
    private final Pattern delimiter;

}
