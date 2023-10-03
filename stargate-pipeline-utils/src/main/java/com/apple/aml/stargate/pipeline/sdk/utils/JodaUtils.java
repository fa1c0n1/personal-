package com.apple.aml.stargate.pipeline.sdk.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

/**
 * Encapsulates joda optional dependency. Instantiates this class only if joda is available on the
 * classpath.
 */
public class JodaUtils {

    private static JodaUtils instance;
    private static boolean instantiated = false;

    public static JodaUtils getConverter() {
        if (instantiated) {
            return instance;
        }

        try {
            Class.forName(
                    "org.joda.time.DateTime",
                    false,
                    Thread.currentThread().getContextClassLoader());
            instance = new JodaUtils();
        } catch (ClassNotFoundException e) {
            instance = null;
        } finally {
            instantiated = true;
        }
        return instance;
    }

    public long convertDate(Object object) {
        final LocalDate value = (LocalDate) object;
        return value.toDate().getTime();
    }

    public int convertTime(Object object) {
        final LocalTime value = (LocalTime) object;
        return value.get(DateTimeFieldType.millisOfDay());
    }

    public long convertTimestamp(Object object) {
        final DateTime value = (DateTime) object;
        return value.toDate().getTime();
    }

    private JodaUtils() {}
}