package nl.jnc.mongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import org.apache.log4j.Logger;

import java.net.UnknownHostException;

public class WordCountMongo {

    private static Logger logger = Logger.getLogger(WordCountMongo.class);

    private DBCollection relativeCollection;
    private String baseName = "test";
    private String collectionIn = "in";
    private String collectionOut = "mongo_out";
    private String mapFunc, reduceFunc;
    private final MapReduceCommand mpCommand;

    public WordCountMongo() throws UnknownHostException {

        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB(baseName);
        this.relativeCollection = db.getCollection(collectionIn);

        this.mapFunc = "function() {emit(this.country, 1);}";

        this.reduceFunc = "function(key, values){return Array.sum(values); }";

        this.mpCommand = new MapReduceCommand(
                relativeCollection,
                mapFunc,
                reduceFunc,
                collectionOut,
                MapReduceCommand.OutputType.REPLACE, null
        );
    }

    public void run() {

        MapReduceOutput mapReduceOutput = relativeCollection.mapReduce(mpCommand);
        DBObject counts = (DBObject) mapReduceOutput.getCommandResult().get("counts");

        System.out.println("Input records = " + counts.get("input"));
        System.out.println("Output records = " + counts.get("output"));
    }

}