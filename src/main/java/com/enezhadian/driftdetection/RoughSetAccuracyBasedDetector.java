/*
 *      RoughSetAccuracyBasedDetector.java
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

package com.enezhadian.driftdetection;

import scala.Tuple2;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Time;


public class RoughSetAccuracyBasedDetector<Type> {

    public RoughSetAccuracyBasedDetector(JavaDStream<Type> stream, double threshold) {
        this.stream = stream;
        this.threshold = threshold;
    }

    public JavaDStream<Boolean> hasDrifted() {
        // Add timestamp to each batch entry to keep track of batches.
        JavaPairDStream streamWithTimestamp = stream.transformToPair((batch, time) ->
                batch.mapToPair((entry) -> new Tuple2<>(time, entry)));

        // Combine each batch with its next batch to compare consecutive batches for concept drift.
        JavaPairDStream consecutiveBatchesStream = streamWithTimestamp.window(Durations.seconds(2));

        consecutiveBatchesStream.transform((twoBatches) -> {
            JavaPairRDD batches = (JavaPairRDD<Time, Type>) twoBatches;

            // TODO: Calculate rough set accuracy for each batch with respect to elements in both.
            return batches;
        });

        return stream.transform((rdd) -> rdd.map((x) -> true));
    }


    private final JavaDStream<Type> stream;
    private final double threshold;

}
