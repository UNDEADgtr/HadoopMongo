package nl.jnc;

import nl.jnc.hadoop.WordCountHadoop;
import nl.jnc.mongo.WordCountMongoAggregation;
import nl.jnc.mongo.WordCountMongoMapReduce;
import nl.jnc.util.Util;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;

public class TestApp {

    private static Logger logger = Logger.getLogger(TestApp.class);

    private static String mongoHost = "localhost";
    private static int mongoPort = 40001;

    private static int numberOfClients = 2000;
    private static int numberOfRequests = 2000;
    private static String dbName = "test2000";
    private static String inCollection = "in";
    private static String outHadoopCollection = "hadoop_out";
    private static String outMongoCollectionMR = "mongo_out_mr";
    private static String outMongoCollectionAggregate = "mongo_out_aggregate";
    private static long absoluteCalculatePeriod = 15000l;
    private static long clientSleepMills = 50l;
    private static boolean launched = true;
    private static int launchCount = 10;

    private static AppConfig appConfig;

    static {
        appConfig = new AppConfig(
                mongoHost,
                mongoPort,
                dbName,
                inCollection,
                outHadoopCollection,
                outMongoCollectionMR,
                outMongoCollectionAggregate,
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
        //this.startClients();
        this.startMapReduceHodoop();
        //this.startMapReduceMongo();
        //this.startMongoAggregate();
    }

    private void startClients() throws UnknownHostException {
        Client.count = numberOfClients;
        for (int i = 0; i < numberOfClients; i++) {
            Client client = new Client(appConfig);
            new Thread(client).start();
        }
    }

    private void startMapReduceHodoop() throws Exception {
        WordCountHadoop wordCount = new WordCountHadoop(appConfig);
        new Thread(wordCount).start();
    }

    private void startMapReduceMongo() throws Exception {
        WordCountMongoMapReduce wordCount = new WordCountMongoMapReduce(appConfig);
        new Thread(wordCount).start();
    }

    private void startMongoAggregate() throws Exception {
        WordCountMongoAggregation wordCount = new WordCountMongoAggregation(appConfig);
        new Thread(wordCount).start();
    }
}