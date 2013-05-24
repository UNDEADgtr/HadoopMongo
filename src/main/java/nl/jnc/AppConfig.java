package nl.jnc;

import java.util.ArrayList;
import java.util.List;

public class AppConfig {

    private String dbName;
    private String inCollection;
    private String outHadoopCollection;
    private String outMongoCollection;
    private long absoluteCalculatePeriodMills;
    private long clientSleepMills;
    private long numberOfRequest;
    private boolean launched;
    private int launchCount;
    private int numberOfClients;
    private List<Double> time;

    public AppConfig(String dbName, String inCollection, String outHadoopCollection, String outMongoCollection,
                     long numberOfRequest, long absoluteCalculatePeriodMills, long clientSleepMills,
                     boolean launched, int launchCount, int numberOfClients) {
        this.dbName = dbName;
        this.inCollection = inCollection;
        this.outHadoopCollection = outHadoopCollection;
        this.outMongoCollection = outMongoCollection;
        this.numberOfRequest = numberOfRequest;
        this.absoluteCalculatePeriodMills = absoluteCalculatePeriodMills;
        this.clientSleepMills = clientSleepMills;
        this.launched = launched;
        this.launchCount = launchCount;
        this.numberOfClients = numberOfClients;
        time = new ArrayList<Double>();
    }

    public String getDbName() {
        return dbName;
    }

    public long getNumberOfRequest() {
        return numberOfRequest;
    }

    public String getInCollection() {
        return inCollection;
    }

    public void setInCollection(String inCollection) {
        this.inCollection = inCollection;
    }

    public String getOutHadoopCollection() {
        return outHadoopCollection;
    }

    public void setOutHadoopCollection(String outHadoopCollection) {
        this.outHadoopCollection = outHadoopCollection;
    }

    public String getOutMongoCollection() {
        return outMongoCollection;
    }

    public void setOutMongoCollection(String outMongoCollection) {
        this.outMongoCollection = outMongoCollection;
    }

    public long getAbsoluteCalculatePeriodMills() {
        return absoluteCalculatePeriodMills;
    }

    public long getClientSleepMills() {
        return clientSleepMills;
    }

    public boolean isLaunched() {
        return launched;
    }

    public void setLaunched(boolean launched) {
        this.launched = launched;
    }

    public int getLaunchCount() {
        return launchCount;
    }

    public void setLaunchCount(int launchCount) {
        this.launchCount = launchCount;
    }

    public List<Double> getTime() {
        return time;
    }

    public void setTime(List<Double> time) {
        this.time = time;
    }

    public void addTime(double time) {
        this.time.add(time);
    }

    public double getAverageTime() {
        double avTime = 0;
        for (double l : time) {
            avTime = avTime + l;
        }
        avTime = avTime - time.get(0);
        return avTime/(time.size() - 1);
    }

    public int getNumberOfClients() {
        return numberOfClients;
    }

    public void setNumberOfClients(int numberOfClients) {
        this.numberOfClients = numberOfClients;
    }
}