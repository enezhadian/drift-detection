/*
 *      StreamKrimp/ClosedFrequentSetMiner.java
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

package StreamKrimp;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.ml.fpm.FPGrowth;


public class ClosedFrequentSetMiner {

    // TODO[2]: Make this a static method instead of forcing users to instantiate it.
    public ClosedFrequentSetMiner(ImmutableList<ImmutableSet> streamSlice) {
        this.streamSlice = streamSlice;

    }

    // TODO[2]: Replace this. At the moment it uses Spark.mllib library to find all the frequent itemsets.
    public ImmutableList<ImmutableSet> nonSingletonClosedFrequentItemsets(double minSupport) {
        // Create Spark session.
        SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
        session.sparkContext().setLogLevel("OFF");

        // Convert Immutable list of sets to list of Spark rows.
        List<Row> data = new ArrayList<>();
        for (ImmutableSet set : streamSlice) {
            data.add(RowFactory.create(set.asList()));
        }

        // Create schema for data frame.
        StructType schema = new StructType(new StructField[]{ new StructField(
                "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });

        // Create a data frame for sets of items.
        Dataset<Row> setsDataFrame = session.createDataFrame(data, schema);

        FPGrowthModel model = new FPGrowth()
                .setItemsCol("items")
                .setMinSupport(minSupport)
                .fit(setsDataFrame);

        List<Row> closedFrequents =  model.freqItemsets().collectAsList();
        ImmutableList.Builder<ImmutableSet> listBuilder = new ImmutableList.Builder<>();
        for (Row row : closedFrequents) {
            ImmutableSet.Builder<String> setBuilder = new ImmutableSet.Builder<>();
            for (Object item : row.getList(0)) {
                setBuilder.add((String) item);
            }
            listBuilder.add(setBuilder.build());
        }

        // TODO[1]: Fix the order of returned itemsets.
        return listBuilder.build();
    }

    public ImmutableList<ImmutableSet> singletons() {
        // TODO[1]: Return singletons.
        return null;
    }

    private final ImmutableList<ImmutableSet> streamSlice;

}
