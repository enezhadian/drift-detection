/*
 *      LineByLineTextFileReceiver.java
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

import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.storage.StorageLevel;


public class LineByLineTextFileReceiver extends Receiver<String> {

    public LineByLineTextFileReceiver(StorageLevel storageLevel, String path,
                                      long lineReadLatencyInMillis) throws FileNotFoundException {
        super(storageLevel);

        this.path = path;
        this.lineReadLatencyInMillis = lineReadLatencyInMillis;
        this.lastLineRead = 0;
    }

    public void onStart() {
        // Start a thread that reads data from a text file.
        new Thread(this::receive).start();
    }

    public void onStop() {
        // There is nothing to do here, as `receive()` already stops when the file is completely
        // read.
    }


    private final String path;
    private final long lineReadLatencyInMillis;
    private long lastLineRead;

    private void receive() {
        try {
            // Open the file.
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(path), StandardCharsets.UTF_8));

            // Read one line every `lineReadLatencyInMillis`
            String line;
            long nextLineToRead = 1;

            while (!isStopped() && (line = reader.readLine()) != null) {
                if (nextLineToRead <= lastLineRead) {
                    continue;
                }
                // Store read line into Spark's memory.
                store(line);
                lastLineRead = nextLineToRead;
                nextLineToRead += 1;
                Thread.sleep(lineReadLatencyInMillis);
            }

            // Close the file.
            reader.close();
        } catch (IOException|InterruptedException e) {
            // Restart reading file.
            restart("Reading interrupted.", e);
        }
    }

}
