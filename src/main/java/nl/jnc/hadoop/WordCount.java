// WordCount.java
/*
 * Copyright 2010 10gen Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.jnc.hadoop;

import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import nl.jnc.AppConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class WordCount implements Runnable {

    private static final Log logger = LogFactory.getLog(WordCount.class);

    private AppConfig appConfig;

    public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {

//            System.out.println("key: " + key);
//            System.out.println("value: " + value);

            final StringTokenizer itr = new StringTokenizer(value.get("country").toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (final IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public WordCount(AppConfig config) {
        this.appConfig = config;
    }

    @Override
    public void run() {
        final Configuration conf = new Configuration();

        MongoConfigUtil.setInputURI(conf, "mongodb://localhost/" + appConfig.getDbName() + "." + appConfig.getInCollection());
        MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/" + appConfig.getDbName() + "." + appConfig.getOutHadoopCollection());

        double oneBillion = 1000000000d;
        try {
            int i=0;
            while (true) {
                Thread.sleep(appConfig.getAbsoluteCalculatePeriodMills());

                Job job = null;
                try {
                    job = new Job(conf, "word count");
                } catch (IOException e) {
                    e.printStackTrace();
                }

                job.setJarByClass(WordCount.class);
                job.setMapperClass(TokenizerMapper.class);
                job.setCombinerClass(IntSumReducer.class);
                job.setReducerClass(IntSumReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                job.setInputFormatClass(MongoInputFormat.class);
                job.setOutputFormatClass(MongoOutputFormat.class);
                //job.setOutputFormatClass(NullOutputFormat.class);

                logger.debug("start Map-Reduce Hodoop...");
                long startTime = System.nanoTime();
                logger.debug("stop Hodoop Map-Reduce: " + job.waitForCompletion(true));

                long endMapReduceTime = System.nanoTime();
                double mapReduceTime = (endMapReduceTime - startTime) / oneBillion;
                logger.debug("Map-Reduce Hodoop time = " + mapReduceTime + " seconds");
                appConfig.addTime(mapReduceTime);
                if (!appConfig.isLaunched()) {
                    break;
                }
//                if (appConfig.getLaunchCount() == i) {
//                    break;
//                }
//                i++;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        logger.debug("      Result time = " + appConfig.getAverageTime());

    }

//    public static void main( String[] args ) throws Exception{
//        WordCount wordCount = new WordCount();
//        wordCount.run();
//    }

}
