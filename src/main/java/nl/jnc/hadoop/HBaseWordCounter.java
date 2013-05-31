package nl.jnc.hadoop;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.bson.types.ObjectId;


/**
 * Created by
 * User: Khralovich Dzmitry
 * Date: 31.05.13
 * Time: 10:40
 */
public class HBaseWordCounter {


    static class Mapper extends TableMapper<Text, IntWritable> {

        private int numRecords = 0;
        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
            ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);

            String country = Bytes.toString(values.value());

            try {
                context.write(new Text(country), one);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            numRecords++;
            if ((numRecords % 10000) == 0) {
                context.setStatus("mapper processed " + numRecords + " records so far");
            }
        }
    }

    public static class Reducer extends TableReducer<Text, IntWritable, Text> {



        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            byte[] row = Bytes.toBytes("row_" + new ObjectId());
            Put put = new Put(row);

            put.add(Bytes.toBytes("count"), Bytes.toBytes("country"), Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("count"), Bytes.toBytes("count"), Bytes.toBytes(""+sum));

            context.write(key, put);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        Job job = new Job(conf, "Hbase_HBaseWordCounter");
        job.setJarByClass(HBaseWordCounter.class);

        byte[] family = Bytes.toBytes("patent");
        byte[] qual = Bytes.toBytes("country");

        Scan scan = new Scan();
        scan.addColumn(family, qual);

        TableMapReduceUtil.initTableMapperJob("inTest", scan, Mapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("outTest", Reducer.class, job);
        TableMapReduceUtil.setScannerCaching(job, 10000);

        System.out.println("start Map-Reduce HBase...");
        long startTime = System.nanoTime();

        job.waitForCompletion(true);

        long endMapReduceTime = System.nanoTime();
        double mapReduceTime = (endMapReduceTime - startTime) / 1000000000d;
        System.out.println("Map-Reduce Mongo time = " + mapReduceTime + " seconds");
    }
}
