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


// TODO[3]: Make this thread-safe to be able to respond to multiple stream processors.
// TODO[3]: Use the smallest integer type rather than keeping multiple copies of each item's name.
/**
 * TODO[4]: Documentation.
 */
public class ItemsetStreamReader<ItemType> {

    // TODO[1]: Singletons should be given to this class.

    /**
     * TODO[4]: Documentation.
     * @param path
     * @param delimiterRegex
     * @throws FileNotFoundException
     */
    public ItemsetStreamReader(String path,
                               String delimiterRegex,
                               List<ItemType> items) throws FileNotFoundException {
        itemType = (Class<?>) getClass().getTypeParameters()[0].getBounds()[0];
        reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        delimiter = Pattern.compile(delimiterRegex);
        head = null;

        ImmutableList.Builder builder = new ImmutableList.Builder();
        for (ItemType item : items) {
            builder.add(new ImmutableSet.Builder().add(item).build());
        }
        singletons = builder.build();
    }

    /**
     * Get transactions from data stream starting after last discarded transaction.
     * @param maxSize Maximum number of of transactions to get.
     * @return head of data stream with size at most `maxSize`.
     */
    public ImmutableList<ImmutableSet> head(int maxSize) throws NoSuchElementException {
        assert(maxSize > 0);

        expandHeadTo(maxSize);
        return head.subList(0, maxSize <= head.size() ? maxSize : head.size());
    }

    /**
     * Discard given number of transactions after last discarded transaction or from the beginning
     * of data stream.
     * TODO[4]: Documentation.
     * @param maxSize
     * @return
     */
    public void discard(int maxSize) {
        assert(maxSize > 0);

        int headSize = head != null ? head.size() : 0;

        if (maxSize < headSize) {
            head = new ImmutableList.Builder<ImmutableSet>()
                    .addAll(head.subList(maxSize, headSize))
                    .build();
        } else {
            head = null;
            skipLines(maxSize - headSize);
        }
    }


    private final Class<?> itemType;
    private final BufferedReader reader;
    private final Pattern delimiter;
    private ImmutableList<ImmutableSet> head;
    private final ImmutableList<ImmutableSet> singletons;

    private ItemType parseItem(String itemString) {
        ItemType item;

        if (itemType.isAssignableFrom(String.class)) {
            item = (ItemType) itemString;
        } else if (itemType.isAssignableFrom(Integer.class)) {
            item = (ItemType) Integer.valueOf(itemString);
        } else if (itemType.isAssignableFrom(Boolean.class)) {
            item = (ItemType) Boolean.valueOf(itemString);
        } else if (itemType.isAssignableFrom(Double.class)) {
            item = (ItemType) Double.valueOf(itemString);
        } else {
            throw new IllegalArgumentException("Unknown item type.");
        }

        return item;
    }

    /**
     * TODO[4]: Documentation.
     * @param maxSize
     * @return
     */
    private void expandHeadTo(int maxSize) throws NoSuchElementException {
        assert(maxSize > 0);

        if (head != null && head.size() >= maxSize) {
            // Head is already big enough.
            return;
        }

        ImmutableList.Builder<ImmutableSet> headBuilder = new ImmutableList.Builder<>();

        if (head != null) {
            headBuilder.addAll(head);
            maxSize -= head.size();
        }

        int size = 0;
        String line;
        try {
            while (size < maxSize && (line = reader.readLine()) != null) {
                size++;
                ImmutableSet.Builder setBuilder = new ImmutableSet.Builder<>();

                for (String itemString : delimiter.split(line)) {
                    setBuilder.add(parseItem(itemString));
                }

                headBuilder.add(setBuilder.build());
            }
        } catch (IOException e) {
            // Simply continue as there is no more transactions to read.
        }

        head = headBuilder.build();

        if (head.size() == 0) {
            throw new NoSuchElementException();
        }
    }

    private void skipLines(int maxSize) {
        assert(maxSize > 0);

        try {
            // Skip `maxSize` lines from input file.
            for (int size = 0; size < maxSize && reader.readLine() != null; size++);
        } catch (IOException e) {
            // Simply continue as there is no more transactions to read.
        }
    }

}
