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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


// TODO: Make this thread-safe to be able to respond to multiple stream processors.
// TODO: Use the smallest integer type rather than keeping multiple copies of each item's name.
public class ItemsetStreamReader {

    public ItemsetStreamReader(String path,
                               String delimiterRegex,
                               List<String> items) throws FileNotFoundException {
        this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        this.delimiter = Pattern.compile(delimiterRegex);
        this.head = null;
        this.items = ImmutableList.<String>builder().addAll(items).build();
    }

    public ImmutableList<String> items() {
        return items;
    }

    public ImmutableList<ImmutableSet<String>> head(int maxSize) throws NoSuchElementException {
        expandHeadTo(maxSize);
        return head.subList(0, maxSize <= head.size() ? maxSize : head.size());
    }

    public void discard(int maxSize) {
        int headSize = head != null ? head.size() : 0;
        int skipSize;

        if (maxSize < headSize) {
            head = ImmutableList.<ImmutableSet<String>>builder()
                    .addAll(head.subList(maxSize, headSize))
                    .build();
        } else {
            head = null;
            skipSize = maxSize - headSize;
            if (skipSize > 0) {
                skipLines(skipSize);
            }
        }
    }

    private final BufferedReader reader;
    private final Pattern delimiter;
    private ImmutableList<ImmutableSet<String>> head;
    private final ImmutableList<String> items;

    public int read = 0;

    private void expandHeadTo(int maxSize) throws NoSuchElementException {
        if (head != null && head.size() >= maxSize) {
            // Head is already big enough.
            return;
        }

        ImmutableList.Builder<ImmutableSet<String>> headBuilder = ImmutableList.builder();

        if (head != null) {
            headBuilder.addAll(head);
            maxSize -= head.size();
        }

        int size = 0;
        String line;
        try {
            while (size < maxSize && (line = reader.readLine()) != null) {
                read++;

                size++;
                ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();

                for (String item : delimiter.split(line)) {
                    setBuilder.add(item);
                }

                headBuilder.add(setBuilder.build());
            }
        } catch (IOException e) {
            // Simply continue as there are no more transactions to be read.
        }

        head = headBuilder.build();

        if (head.size() == 0) {
            throw new NoSuchElementException();
        }
    }

    private void skipLines(int maxSize) {
        System.out.println("Skipping " + maxSize + " lines without processing.");
        try {
            // Skip `maxSize` lines from input file.
            for (int size = 0; size < maxSize && reader.readLine() != null; size++) {
                read++;
            }
        } catch (IOException e) {
            // Simply continue as there are no more transactions to be read.
        }
    }

}
