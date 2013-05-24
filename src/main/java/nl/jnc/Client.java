package nl.jnc;

import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Client implements Runnable {

    private static Logger logger = Logger.getLogger(Client.class);

    private DBCollection collection;
    private String deviceId;
    private AppConfig appConfig;

    public static long maxTime;
    public static long minTime;
    public static List<Long> totalTime;
    public static int count;

    public Client(AppConfig config) throws UnknownHostException {
        this.appConfig = config;
        Mongo mongo = new MongoClient(appConfig.getMongoHost(), appConfig.getMongoPort());
        this.collection = mongo.getDB(config.getDbName()).getCollection(config.getInCollection());
        this.deviceId = new ObjectId().toString();
        this.totalTime = new ArrayList<Long>();
        this.maxTime = Long.MIN_VALUE;
        this.minTime = Long.MAX_VALUE;
    }

    @Override
    public void run() {
        long startClientTime = System.nanoTime();
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;
        String msg = "client with device id=%s started";
        logger.debug(String.format(msg, deviceId));
        for (int i = 0; i < this.appConfig.getNumberOfRequest(); i++) {
            long startTime = System.nanoTime();
            this.insert();
            long time = System.nanoTime() - startTime;
            if (time > max) {
                max = time;
            }
            if (time < min) {
                min = time;
            }
            try {
                Thread.sleep(appConfig.getClientSleepMills());
            } catch (InterruptedException e) {
                logger.error(e);
            }

        }
        long endClientTime = System.nanoTime();
        msg = "client with device id=%s stopped. Max insert time=%s nano seconds. Min insert time=%s nano seconds." +
                "Running client time: %s nano seconds";
        logger.debug(String.format(msg, deviceId, max, min, endClientTime - startClientTime));

        if (max > maxTime) {
            maxTime = max;
        }
        if (min < minTime) {
            minTime = min;
        }
        totalTime.add(endClientTime - startClientTime);
        result();
    }

    public void result() {
        if (count == 1) {
            long total = 0;
            for (long l : Client.totalTime) {
                total = total + l;
            }
            logger.debug("      Max client time = " + Client.maxTime);
            logger.debug("      Min client time = " + Client.minTime);
            logger.debug("      Total client time = " + (total / Client.totalTime.size()));
            logger.debug("      Average client time = " +
                    (total / Client.totalTime.size() - appConfig.getNumberOfRequest() * appConfig.getClientSleepMills() * 1000000) /
                            appConfig.getNumberOfRequest() + " sec per rec");
        } else {
            count--;
        }
    }

    public void insert() {
        String patent = "123456789";
        String date = "2013";
        String country = getRandomCountry();
        String code = "3";
        PatentData patentData = new PatentData(patent, date, country, code);
        collection.insert(patentData.getDBObject(), WriteConcern.SAFE);
    }

    private String[] countries = new String[]{"US", "RU", "BE", "GB", "UA"};

    private String getRandomCountry() {
        Random random = new Random();
        return countries[random.nextInt(countries.length)];
    }
}