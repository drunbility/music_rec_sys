/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package music_rec_cf;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class FilterJob {

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataSet<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/batch/index.html
         *
         * and the examples
         *
         * https://flink.apache.org/docs/latest/apis/batch/examples.html
         *
         */

        // execute program


        String path = FilterJob.class.getClassLoader().getResource("tmusic.data").getPath();

        DataSet<String> text = env.readTextFile(path);


        FlatMapOperator<String, Tuple3<String, String, Double>> tup1 = text.flatMap(new Spliter());

        String sink = "src/main/resources/tmusic1.data";

        tup1.groupBy(0).reduceGroup(new Filter()).writeAsCsv(sink, "\n", "\001");

        env.execute("Flink Batch Java API Skeleton");
    }


    public static class Filter implements GroupReduceFunction<Tuple3<String, String, Double>, Tuple3<String, String, Double>> {

        @Override
        public void reduce(Iterable<Tuple3<String, String, Double>> iterable, Collector<Tuple3<String, String, Double>> collector) throws Exception {

            List<Tuple2<String, Double>> scores = new ArrayList<>();
            String token = "";
            String item = "";
            Double score = 0d;


            for (Tuple3<String, String, Double> t : iterable) {
                token = t.f0;
                item = t.f1;
                score = t.f2;

                scores.add(new Tuple2<>(item, score));
            }

            if (scores.size() < 10) {
                for (Tuple2<String, Double> t : scores) {

                    collector.collect(new Tuple3<>(token, t.f0, t.f1));
                }

            }


        }
    }


    public static class Spliter implements FlatMapFunction<String, Tuple3<String, String, Double>> {

        @Override
        public void flatMap(String s, Collector<Tuple3<String, String, Double>> collector) throws Exception {
            String[] tokens = s.split("\001");

            collector.collect(new Tuple3<>(tokens[0], tokens[1], Double.valueOf(tokens[2])));

        }
    }


}
