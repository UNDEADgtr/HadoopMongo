package nl.jnc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by
 * User: Khralovich Dzmitry
 * Date: 30.05.13
 * Time: 17:44
 */
public class HBaseRowCounter{
    /**
     * Name of this 'program'.
     */
    static final String NAME = "rowcounter";

    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        /**
         * Counter enumeration to count the actual rows.
         */
        public static enum Counters {
            ROWS
        }
    }

    public void map(ImmutableBytesWritable row, Result values, Mapper.Context context) throws IOException {
        for (KeyValue value : values.list()) {
            if (value.getValue().length > 0) {
                context.getCounter(HBaseRowCounter.RowCounterMapper.Counters.ROWS).increment(1);
                break;
            }
        }
    }

    public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
        String tableName = args[0];
        Job job = new Job(conf, NAME + "_" + tableName);
        job.setJarByClass(HBaseRowCounter.class);
// Columns are space delimited
        StringBuilder sb = new StringBuilder();
        final int columnoffset = 1;
        for (int i = columnoffset; i < args.length; i++) {
            if (i > columnoffset) {
                sb.append(" ");
            }
            sb.append(args[i]);
        }
        Scan scan = new Scan();
        scan.setFilter(new FirstKeyOnlyFilter());
        if (sb.length() > 0) {
            for (String columnName : sb.toString().split(" ")) {
                String[] fields = columnName.split(":");
                if (fields.length == 1) {
                    scan.addFamily(Bytes.toBytes(fields[0]));
                } else {
                    scan.addColumn(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1]));
                }
            }
        }
// Second argument is the table name.
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(tableName, scan,
                RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
        job.setNumReduceTasks(0);
        return job;
    }

    public static void main(String[] args) throws Exception {
        double oneBillion = 1000000000d;

        String[] base = new String[]{ "test"};
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, base).getRemainingArgs();
        if (otherArgs.length < 1) {
            System.err.println("ERROR: Wrong number of parameters: " + args.length);
            System.err.println("Usage: RowCounter <tablename> [<column1> <column2>...]");
            System.exit(-1);
        }
        Job job = createSubmittableJob(conf, otherArgs);


        System.out.println("start HBase Map-Reduce...");
        long startTime = System.nanoTime();

        job.waitForCompletion(true);

        long endMapReduceTime = System.nanoTime();
        double mapReduceTime = (endMapReduceTime - startTime) / oneBillion;

        String timeString = "Map-Reduce Hodoop time = " + mapReduceTime + " seconds";
        System.out.println(timeString);

//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
