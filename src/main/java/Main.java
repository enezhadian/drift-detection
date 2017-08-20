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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import DataStreamReader.ItemsetStreamReader;
import StreamKrimp.CodeTable;


public class Main {

    public static void main(String[] args) throws Exception {
        ItemsetStreamReader stream = new ItemsetStreamReader("data/stream.txt", "\\s");
        ImmutableList<ImmutableSet> batch = stream.head(1000);
        CodeTable.findCandidatesFor(batch, 0.1, new ArrayList<>(), new ArrayList<>());
    }

}
