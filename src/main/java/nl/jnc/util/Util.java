package nl.jnc.util;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import nl.jnc.PatentData;

import java.io.FileReader;
import java.io.IOException;
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

        Scanner scanner = new Scanner(new FileReader("/home/dzmity/IdeaProjects/HadoopTest/src/main/resources/nl/nl.jnc/nl.nl.jnc.util/apat.txt"));
        Mongo mongo = new MongoClient();
        DBCollection collection = mongo.getDB("test").getCollection("in");
        scanner.nextLine();
        int count = 0;
        System.out.println("Start import");
        List<DBObject> list = new ArrayList<DBObject>(1000);
        while (scanner.hasNextLine()) {
            String[] columns = scanner.nextLine().split(",");
            PatentData patent = new PatentData();
            patent.setPatent(columns[0]);
            patent.setYear(columns[1]);
            patent.setCountry(columns[4].replace("\"", ""));
            patent.setCode(columns[7]);
            list.add(patent.getDBObject());
            if (list.size() == 1000) {
                collection.insert(list);
                list.clear();
            }
            count++;
        }
        System.out.println("Finish import");
        System.out.println(String.format("Imported %s records", count));
//        InputStream is = Util.class.getResourceAsStream("test.xml");
//        InputStream stream = this.getClass().getResourceAsStream("test.xml");
//        InputStreamReader reader = new InputStreamReader(stream);
//        BufferedReader buff = new BufferedReader(reader);
//        String s = buff.readLine();
//        while ((s = buff.readLine()) != null) {
//        }
    }

}
