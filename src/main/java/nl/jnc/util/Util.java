package nl.jnc.util;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import nl.jnc.PatentData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by
 * User: Khralovich Dzmitry
 * Date: 22.05.13
 * Time: 14:41
 */
public class Util {

    public void importToBase() throws IOException {

        //Scanner scanner = new Scanner(new FileReader("/home/dzmity/IdeaProjects/HadoopTest/src/main/resources/nl/jnc/util/apat.txt"));

        Class clazz = this.getClass();
        InputStream stream;
        Scanner scanner;

        Mongo mongo = new MongoClient();
        DBCollection collection = mongo.getDB("test").getCollection("in");
        int count = 0;
        System.out.println("Start import");
        long startTime = System.nanoTime();

        List<DBObject> list = new ArrayList<DBObject>(10000);

        for (int i = 0; i < 5; i++) {
            stream = clazz.getResourceAsStream("apat.txt");
            scanner = new Scanner(stream);
            scanner.nextLine();
            while (scanner.hasNextLine()) {
                String[] columns = scanner.nextLine().split(",");
                PatentData patent = new PatentData();
                patent.setPatent(columns[0]);
                patent.setYear(columns[1]);
                patent.setCountry(columns[4].replace("\"", ""));
                patent.setCode(columns[7]);
                list.add(patent.getDBObject());
                if (list.size() == 10000) {
                    collection.insert(list);
                    list.clear();
                }
                count++;
//                if(count == 100000){
//                    break;
//                }
            }
        }

        long endMapReduceTime = System.nanoTime();
        double mapReduceTime = (endMapReduceTime - startTime) / 1000000000;

        System.out.println("Finish import");
        System.out.println(String.format("Imported %s records", count));
        System.out.println(String.format("Import  time = %s c", mapReduceTime));
    }

}
