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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.*;


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
public class BatchJob {

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


        String path = BatchJob.class.getClassLoader().getResource("tmusic1.data").getPath();

        DataSet<String> text = env.readTextFile(path);


        FlatMapOperator<String, Tuple3<String, String, Double>> tup1 = text.flatMap(new Spliter());


        GroupReduceOperator<Tuple3<String, String, Double>, Tuple3<String, String, Float>> norm = tup1.groupBy(0).reduceGroup(new Normaliz());

        GroupReduceOperator<Tuple3<String, String, Float>, Tuple2<String, Float>> pairs = norm.groupBy(0).reduceGroup(new Pairs());
//
        MapOperator<Tuple2<String, Float>, Tuple3<String, String, Float>> result = pairs.groupBy(0).sum(1).map(new ResultSplit());


        String rePath = "src/main/resources/similar.data";
        result.writeAsCsv(rePath, "\n", "\001");

        env.execute("music_cf");
    }

    public static class ResultSplit implements MapFunction<Tuple2<String, Float>, Tuple3<String, String, Float>> {

        @Override
        public Tuple3<String, String, Float> map(Tuple2<String, Float> tp2) throws Exception {

            String[] split = tp2.f0.split("\002");

            return new Tuple3<>(split[0], split[1], tp2.f1);
        }
    }

    public static class Pairs implements GroupReduceFunction<Tuple3<String, String, Float>, Tuple2<String, Float>> {

        @Override
        public void reduce(Iterable<Tuple3<String, String, Float>> iterable, Collector<Tuple2<String, Float>> collector) throws Exception {

            List<Tuple2<String, Float>> scores = new ArrayList<>();
            String item = "";
            Float score = 0f;
            for (Tuple3<String, String, Float> t : iterable) {
                item = t.f1;
                score = t.f2;
                scores.add(new Tuple2<>(item, score));
            }

            Float si = 0f;
            String i1 = "";
            String i2 = "";
            BigDecimal b = null;
            for (int i = 0; i < scores.size() - 1; i++) {
                for (int j = i + 1; j < scores.size(); j++) {
                    si = scores.get(i).f1 * scores.get(j).f1;
//                    b=new BigDecimal(si);
//                    si=b.setScale(5,BigDecimal.ROUND_HALF_UP).floatValue();
                    i1 = scores.get(i).f0;
                    i2 = scores.get(j).f0;

                    collector.collect(new Tuple2<>(i2 + "\002" + i1, si));
                    collector.collect(new Tuple2<>(i1 + "\002" + i2, si));
                }
            }
        }
    }

    public static class Normaliz implements GroupReduceFunction<Tuple3<String, String, Double>, Tuple3<String, String, Float>> {

        @Override
        public void reduce(Iterable<Tuple3<String, String, Double>> iterable, Collector<Tuple3<String, String, Float>> collector) throws Exception {
            Double sum = 0d;
            List<Tuple2<String, Double>> scores = new ArrayList<>();
            String token = "";
            String item = "";
            Double score = 0d;

            for (Tuple3<String, String, Double> t : iterable) {
                token = t.f0;
                item = t.f1;
                score = t.f2;
                sum = sum + Math.pow(score, 2);
                scores.add(new Tuple2<>(item, score));
            }

            BigDecimal b = null;
//            sum=b.setScale(6,BigDecimal.ROUND_HALF_UP).doubleValue();
            Float d = 0f;
            for (Tuple2<String, Double> t : scores) {
                b = new BigDecimal(t.f1 / sum);
                d = b.setScale(6, BigDecimal.ROUND_HALF_UP).floatValue();
                collector.collect(new Tuple3<>(token, t.f0, d));
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
