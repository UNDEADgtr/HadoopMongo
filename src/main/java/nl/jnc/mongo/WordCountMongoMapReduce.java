package nl.jnc.mongo;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import nl.jnc.AppConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WordCountMongoMapReduce implements Runnable {

    private static final Log logger = LogFactory.getLog(WordCountMongoMapReduce.class);
    private AppConfig appConfig;

    private DBCollection relativeCollection;
    private String mapFunc, reduceFunc;
    private final MapReduceCommand mpCommand;

    public WordCountMongoMapReduce(AppConfig appConfig) throws Exception {

        this.appConfig = appConfig;
        MongoClient mongoClient = new MongoClient(appConfig.getMongoHost(), appConfig.getMongoPort());
        DB db = mongoClient.getDB(appConfig.getDbName());
        this.relativeCollection = db.getCollection(appConfig.getInCollection());

        this.mapFunc = "function() {emit(this.country, 1);}";

        this.reduceFunc = "function(key, values){return Array.sum(values); }";

        this.mpCommand = new MapReduceCommand(
                relativeCollection,
                mapFunc,
                reduceFunc,
                appConfig.getOutMongoCollection(),
                MapReduceCommand.OutputType.REPLACE, null
        );
    }

    @Override
    public void run() {
        double oneBillion = 1000000000d;
        try {
            int i = 0;
            while (true) {
                Thread.sleep(appConfig.getAbsoluteCalculatePeriodMills());
                logger.debug("start Map-Reduce Mongo...");
                long startTime = System.nanoTime();
                MapReduceOutput mapReduceOutput = relativeCollection.mapReduce(mpCommand);
                long endMapReduceTime = System.nanoTime();
                double mapReduceTime = (endMapReduceTime - startTime) / oneBillion;
                logger.debug("Map-Reduce Mongo time = " + mapReduceTime + " seconds");
                DBObject counts = (DBObject) mapReduceOutput.getCommandResult().get("counts");
                logger.debug("Input records = " + counts.get("input"));
                logger.debug("Output records = " + counts.get("output"));
                appConfig.addTime(mapReduceTime);
                if (!appConfig.isLaunched()) {
                    break;
                }
//                if (appConfig.getLaunchCount() == i) {
//                    break;
//                }
//                i++;
            }
        } catch (InterruptedException e) {
            logger.error(e);
        }
        logger.debug("      Result time = " + appConfig.getAverageTime());
    }

}