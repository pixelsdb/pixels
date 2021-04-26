package io.pixelsdb.pixels.core.utils;

import java.util.Calendar;

/**
 * Created at: 26/04/2021
 * Author: hank
 */
public class DatetimeUtils
{
    private static final int TIMEZONE_OFFSET;

    static
    {
        Calendar calendar = Calendar.getInstance();
        TIMEZONE_OFFSET = -(calendar.get(Calendar.ZONE_OFFSET) +
                calendar.get(Calendar.DST_OFFSET)) / (60 * 1000);
    }

    public static long dayToMillis (int day)
    {
        return day*86400000L+TIMEZONE_OFFSET;
    }

    public static int millisToDay (long millis)
    {
        return (int)((millis-TIMEZONE_OFFSET)/86400000);
    }
}
