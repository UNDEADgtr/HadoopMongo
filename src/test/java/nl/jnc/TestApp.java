package nl.jnc;

import nl.jnc.hadoop.WordCount;
import nl.jnc.mongo.WordCountMongo;
import nl.jnc.util.Util;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.UnknownHostException;

public class TestApp {

    private static Logger logger = Logger.getLogger(TestApp.class);

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
        this.startMapReduceHodoop();
        this.startMapReduceMongo();
    }

    private void startMapReduceHodoop() throws IOException, InterruptedException, ClassNotFoundException {

        logger.debug("start Hodoop Map-Reduce...");

        WordCount wordCount = new WordCount();
        long startTime = System.nanoTime();
        wordCount.run();
        long endMapReduceTime = System.nanoTime();
        double mapReduceTime = (endMapReduceTime - startTime) / 1000000000;

        logger.debug("Map-Reduce time = " + mapReduceTime + " seconds");
    }


    private void startMapReduceMongo() throws UnknownHostException {

        logger.debug("start Mongo Map-Reduce...");

        WordCountMongo wordCountMongo = new WordCountMongo();
        long startTime = System.nanoTime();
        wordCountMongo.run();
        long endMapReduceTime = System.nanoTime();
        double mapReduceTime = (endMapReduceTime - startTime) / 1000000000;

        logger.debug("Map-Reduce time = " + mapReduceTime + " seconds");
    }


}