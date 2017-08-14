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

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public final class Main {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        // Create the context with a 1 second batch size.
        SparkConf conf = new SparkConf().setAppName("Drift Detection");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(1));

        // Set logging level.
        context.sparkContext().setLogLevel("ERROR");

        JavaDStream<String> stream = context.receiverStream(new LineByLineTextFileReceiver(
                StorageLevel.MEMORY_AND_DISK(), "data/input.txt", 200));
        stream.foreachRDD((r, t) -> System.out.println(r.count() + " @ " + t));

        context.start();
        context.awaitTermination();
    }

}
