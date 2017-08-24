/*
 *      DataStreamReader/DataStreamReader.java
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

import com.google.common.collect.ImmutableList;

import java.util.NoSuchElementException;


abstract class DataStreamReader<Type> {

    public ImmutableList<Type> head(int maxSize) throws NoSuchElementException {
        expandHeadTo(maxSize);
        return head.subList(0, maxSize <= head.size() ? maxSize : head.size());
    }

    public void discard(int maxSize) {
        int headSize = head != null ? head.size() : 0;
        int skipSize;

        if (maxSize < headSize) {
            head = ImmutableList.<Type>builder()
                    .addAll(head.subList(maxSize, headSize))
                    .build();
        } else {
            head = null;
            skipSize = maxSize - headSize;
            if (skipSize > 0) {
                System.out.println("Skipping " + skipSize + " lines without processing.");
                skipLines(skipSize);
            }
        }
    }

    protected ImmutableList<Type> head;

    protected abstract void expandHeadTo(int maxSize) throws NoSuchElementException;
    protected abstract void skipLines(int maxSize);

}
