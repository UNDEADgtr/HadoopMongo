package nl.jnc;

import nl.jnc.hadoop.WordCount;
import nl.jnc.mongo.WordCountMongo;
import nl.jnc.util.Util;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;

public class TestApp {

    private static Logger logger = Logger.getLogger(TestApp.class);

    private static int numberOfClients = 2000;
    private static int numberOfRequests = 2000;
    private static String dbName = "test";
    private static String inCollection = "in";
    private static String outHadoopCollection = "hadoop_out";
    private static String outMongoCollection = "mongo_out";
    private static long absoluteCalculatePeriod = 1000l;
    private static long clientSleepMills = 50l;
    private static boolean launched = true;
    private static int launchCount = 10;

    private static AppConfig appConfig;

    static {
        appConfig = new AppConfig(dbName,
                inCollection,
                outHadoopCollection,
                outMongoCollection,
                numberOfRequests,
                absoluteCalculatePeriod,
                clientSleepMills,
                launched,
                launchCount,
                numberOfClients
        );
    }

    public static AppConfig getAppConfig() {
        return appConfig;
    }

    public static void main(String[] args) throws Exception {
//        importBase();
        TestApp testApp = new TestApp();
        testApp.test();
    }

    public static void importBase() throws IOException {
        Util util = new Util();
        util.importToBase();
    }

    public void test() throws Exception {
        logger.debug("starting test...");
//        this.startClients();
        this.startMapReduceHodoop();
//        this.startMapReduceMongo();
    }

    private void startClients() throws UnknownHostException {
        Client.count = numberOfClients;
        for (int i = 0; i < numberOfClients; i++) {
            Client client = new Client(appConfig);
            new Thread(client).start();
        }
    }

    private void startMapReduceHodoop() throws Exception {
        WordCount wordCount = new WordCount(appConfig);
        new Thread(wordCount).start();
    }


    private void startMapReduceMongo() throws Exception {
        WordCountMongo wordCount = new WordCountMongo(appConfig);
        new Thread(wordCount).start();
    }


}