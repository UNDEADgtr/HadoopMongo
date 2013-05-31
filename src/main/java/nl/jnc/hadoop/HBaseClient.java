package nl.jnc.hadoop;

import nl.jnc.util.CountryUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by
 * User: Khralovich Dzmitry
 * Date: 30.05.13
 * Time: 17:23
 */
public class HBaseClient {
    private static final Configuration config = HBaseConfiguration.create();
    private static final byte[] tableName = Bytes.toBytes("in");
    private static final String rowName = "row";
    private static final int i = 0;

    public static void main(String[] args) throws IOException {

        HBaseClient client = new HBaseClient();
        client.createTable();
        client.insertRecords();

//        Get g = new Get(row);
//        Result result = table.get(g);
//        System.out.println("Get: " + result);
//        Scan scan = new Scan();
//        ResultScanner scanner = table.getScanner(scan);
//        try {
//            for (Result scannerResult: scanner) {
//                System.out.println("Scan: " + scannerResult);
//            }
//        } finally {
//            scanner.close();
//        }

//// Drop the table
//        admin.disableTable(tablename);
//        admin.deleteTable(tablename);
    }

    public void createTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(config);
        HTableDescriptor newTable = new HTableDescriptor(tableName);
        HColumnDescriptor patent = new HColumnDescriptor("patent");
        newTable.addFamily(patent);
        admin.createTable(newTable);
        byte[] tableName = newTable.getName();
        HTableDescriptor[] tables = admin.listTables();
        if (tables.length != 1 && Bytes.equals(tableName, tables[0].getName())) {
            throw new IOException("Failed create of table");
        }
    }

    public void insertRecords() throws IOException {
        HTable table = new HTable(config, tableName);
        int i = 0;
        long startTime = System.nanoTime();
        byte[] dataPatent = Bytes.toBytes("patent");
        byte[] row;
        Put put;
        List<Put> puts = new ArrayList<Put>();
        for (i = 0; i < 4000000; i++) {
            row = Bytes.toBytes(rowName + i);
            put = new Put(row);
            put.add(dataPatent, Bytes.toBytes("country"), Bytes.toBytes(CountryUtil.getRandomCountry()));
            put.add(dataPatent, Bytes.toBytes("patent"), Bytes.toBytes("123456789"));
            put.add(dataPatent, Bytes.toBytes("year"), Bytes.toBytes("2013"));
            put.add(dataPatent, Bytes.toBytes("code"), Bytes.toBytes("3"));
            puts.add(put);
            if (puts.size() == 10000) {
                table.put(puts);
                puts.clear();
            }
        }

        long endMapReduceTime = System.nanoTime();
        double insertTime = (endMapReduceTime - startTime) / 1000000000;

        System.out.println("Finish import");
        System.out.println(String.format("Imported %s records", i));
        System.out.println(String.format("Import  time = %s c", insertTime));

    }


}
