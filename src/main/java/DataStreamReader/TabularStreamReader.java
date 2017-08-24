/*
 *      DataStreamReader/CSVStreamReader.java
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;

import org.apache.commons.csv.*;



public final class TabularStreamReader extends DataStreamReader<ImmutableList<String>> {

    /*--------------------------------------------------------------------------*
     *                       INSTANCE MEMBERS AND METHODS                       *
     *--------------------------------------------------------------------------*/

    public TabularStreamReader(String path, boolean hasHeader) throws IOException {
        CSVParser parser = CSVParser.parse(new File(path), Charset.forName("UTF-8"), CSVFormat.RFC4180);
        this.records = parser.iterator();

        if (hasHeader) {
            skipLines(1);
        }
    }

    @Override
    protected void expandHeadTo(int maxSize) throws NoSuchElementException {
        if (head != null && head.size() >= maxSize) {
            // Head is already big enough.
            return;
        }

        ImmutableList.Builder<ImmutableList<String>> headBuilder = ImmutableList.builder();

        if (head != null) {
            headBuilder.addAll(head);
            maxSize -= head.size();
        }

        for (int size = 0; size < maxSize && records.hasNext(); size++) {
            ImmutableList.Builder<String> rowBuilder = ImmutableList.builder();
            for (String value : records.next()) {
                rowBuilder.add(value);
            }
            headBuilder.add(rowBuilder.build());
        }

        head = headBuilder.build();

        if (head.size() == 0) {
            throw new NoSuchElementException();
        }
    }

    @Override
    protected void skipLines(int maxSize) {
        // Skip `maxSize` records.
        for (int size = 0; size < maxSize && records.hasNext(); size++) {
            records.next();
        }
    }

    private final Iterator<CSVRecord> records;

}
