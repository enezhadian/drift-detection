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

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public final class Main {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Missing arguments: <hostname> <port>");
            System.exit(1);
        }

        // Create the context with a 1 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("Network Word Count");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        // Set log level.
        ssc.sparkContext().setLogLevel("ERROR");

        // Create a JavaReceiverInputDStream on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        // Note that no duplication in storage level only for running locally.
        // Replication necessary in distributed scenario for fault tolerance.
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }
}
