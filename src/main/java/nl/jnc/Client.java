package nl.jnc;

import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import nl.jnc.util.CountryUtil;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

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
        String msgTemplate = "client with device id=%s started";
        String msg = String.format(msgTemplate, deviceId);
        logger.debug(msg);
        System.out.println(msg);
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
        collection.getDB().getMongo().close();
        long endClientTime = System.nanoTime();
        msgTemplate = "client with device id=%s stopped. Max insert time=%s nano seconds. Min insert time=%s nano seconds." +
                "Running client time: %s nano seconds";

        msg = String.format(msgTemplate, deviceId, max, min, endClientTime - startClientTime);
        logger.debug(msg);
        System.out.println(msg);

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
            StringBuilder sb = new StringBuilder();
            sb.append("\n      Max insert client time = " + Client.maxTime);
            sb.append("\n      Min insert client time = " + Client.minTime);
            sb.append("\n      Total client time = " + (total / Client.totalTime.size()));
            sb.append("\n      Average client time = " +
                    ((total / Client.totalTime.size() - appConfig.getNumberOfRequest() * appConfig.getClientSleepMills() * 1000000) /
                            appConfig.getNumberOfRequest()) + " sec per rec");
            logger.debug(sb);
            String manySymbols = "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!";
            System.out.println(manySymbols);
            System.out.println(sb);
            System.out.println(manySymbols);

        } else {
            count--;
        }
    }

    public void insert() {
        String patent = "123456789";
        String date = "2013";
        String country = CountryUtil.getRandomCountry();
        String code = "3";
        PatentData patentData = new PatentData(patent, date, country, code);
        collection.insert(patentData.getDBObject(), WriteConcern.SAFE);
    }
}