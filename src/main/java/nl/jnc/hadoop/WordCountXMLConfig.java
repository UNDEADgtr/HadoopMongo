// WordCountXMLConfig.java
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

import com.mongodb.hadoop.util.MongoTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountXMLConfig extends MongoTool {

    public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {

            System.out.println("key: " + key);
            System.out.println("value: " + value);

            final StringTokenizer itr = new StringTokenizer(value.get("x").toString());
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

    static {
        // Load the XML config defined in hadoop-local.xml
        Configuration.addDefaultResource("nl/nl.jnc/settings/hadoop-local.xml");
        Configuration.addDefaultResource("nl/nl.jnc/settings/mongo-wordcount.xml");
    }

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new WordCountXMLConfig(), args);
        System.exit(exitCode);
    }
}
