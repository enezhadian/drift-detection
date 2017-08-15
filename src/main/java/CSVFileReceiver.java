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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.storage.StorageLevel;
import org.apache.commons.csv.*;


public class CSVFileReceiver extends Receiver<String> {

    public CSVFileReceiver(StorageLevel storageLevel, String path, Charset charset,
                           CSVFormat format, short recordsPerSecond) throws FileNotFoundException {
        super(storageLevel);

        this.path = path;
        this.charset = charset;
        this.format = format;
        this.recordsPerSecond = recordsPerSecond;
        this.lastRecordRead = 0;
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
    private final Charset charset;
    private final CSVFormat format;
    private final int recordsPerSecond;
    private long lastRecordRead;

    // TODO: Improve precision of reading.
    private void receive() {
//        try {
//            // Open the CSV file.
//            File csvFile = new File(path);
//            CSVParser parser = CSVParser.parse(csvFile, charset, format);
//
//            Iterator<CSVRecord> records = parser.iterator();
//
//            // Skip already-read lines.
//            for (int i = 0; !isStopped() && i < lastRecordRead && records.hasNext(); i++) {
//                records.next();
//            }
//
//            // Read a batch every second.
//            String line;
//            long loopStartTime, sleepTime;
//
//            while (!isStopped()) {
//                loopStartTime = System.currentTimeMillis();
//
//                for (short i = 0; i < recordsPerSecond; i++) {
//                    if (!records.hasNext()) {
//                        stop("All records have been read.");
//                        break;
//                    }
//                    CSVRecord record = records.next();
//
//                    // Store read line into Spark's memory.
//                    store(record);
//                    lastRecordRead++;
//                }
//
//                sleepTime = 1000 - System.currentTimeMillis() + loopStartTime;
//                Thread.sleep(sleepTime);
//            }
//        } catch (IOException|InterruptedException e) {
//            // Restart reading file.
//            restart("Reading interrupted.", e);
//        }
    }

}
