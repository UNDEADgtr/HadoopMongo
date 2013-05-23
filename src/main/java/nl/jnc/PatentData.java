package nl.jnc;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class PatentData {

    public static final String PATENT = "patent";
    public static final String YEAR = "year";
    public static final String COUNTRY = "country";
    public static final String CODE = "code";

    private DBObject dbObject;

    public PatentData() {
        this.dbObject = new BasicDBObject();
    }

    public PatentData(String patent, String year, String country, String code) {
        this();
        this.setPatent(patent);
        this.setYear(year);
        this.setCountry(country);
        this.setCode(code);
    }

    public String getPatent() {
        return (String) dbObject.get(PATENT);
    }

    public void setPatent(String patent) {
        dbObject.put(PATENT, patent);
    }

    public String getYear() {
        return (String) dbObject.get(YEAR);
    }

    public void setYear(String year) {
        dbObject.put(YEAR, year);
    }

    public String getCountry() {
        return (String) dbObject.get(COUNTRY);
    }

    public void setCountry(String country) {
        dbObject.put(COUNTRY, country);
    }

    public String getCode() {
        return (String) dbObject.get(CODE);
    }

    public void setCode(String code) {
        dbObject.put(CODE, code);
    }

    public DBObject getDBObject() {
        return this.dbObject;
    }
}