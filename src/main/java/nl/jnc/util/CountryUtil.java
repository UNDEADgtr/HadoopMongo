package nl.jnc.util;

import java.util.Locale;
import java.util.Random;

public class CountryUtil {

    private static String[] countries = Locale.getISOCountries();
    private static Random random = new Random();

    public static String getRandomCountry() {
        return countries[random.nextInt(countries.length)];
    }
}