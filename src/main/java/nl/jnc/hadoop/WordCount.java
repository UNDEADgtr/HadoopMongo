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

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class WordCount {

    private static final Log log = LogFactory.getLog(WordCount.class);

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

    public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();

        MongoConfigUtil.setInputURI(conf, "mongodb://localhost/test.in");
        MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/test.hadoop_out");

        System.out.println("Conf: " + conf);

        final Job job = new Job(conf, "word count");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);

        return job.waitForCompletion(true) ? false : true;
    }

}
