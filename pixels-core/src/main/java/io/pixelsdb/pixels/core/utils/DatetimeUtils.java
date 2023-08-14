/*
 * Copyright 2021 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.utils;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.TimeZone;

/**
 * @author hank
 * @create 2021-04-26
 */
public class DatetimeUtils
{
    private static long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();

    private static final long MICROS_PER_MILLIS = 1000L;
    private static final long NANOS_PER_MILLIS = 1000_000L;
    private static final long MICROS_PER_SEC = 1000_000L;
    private static final long NANOS_PER_MICROS = 1000L;

    public static long microsToMillis(long micros)
    {
        return micros / MICROS_PER_MILLIS;
    }

    public static int microsToFracNanos(long micros)
    {
        return (int) (micros % MICROS_PER_SEC * NANOS_PER_MICROS);
    }

    public static long timestampToMicros(Timestamp timestamp)
    {
        return timestamp.getTime() * MICROS_PER_MILLIS +
                timestamp.getNanos() % NANOS_PER_MILLIS / NANOS_PER_MICROS;
    }

    private static final long[] PRECISION_ROUND_FACTOR_FOR_MICROS = {
            1000_000L, 100_000L, 10_000L, 1000L, 100L, 10L, 1L};

    private static final int[] PRECISION_ROUND_FACTOR_FOR_MILLIS = {
            1000, 100, 10, 1};

    public static long roundMicrosToPrecision(long micros, int precision)
    {
        long roundFactor = PRECISION_ROUND_FACTOR_FOR_MICROS[precision];
        return micros / roundFactor * roundFactor;
    }

    public static int roundMillisToPrecision(int millis, int precision)
    {
        int roundFactor = PRECISION_ROUND_FACTOR_FOR_MILLIS[precision];
        return millis / roundFactor * roundFactor;
    }

    public static void resetTimezoneOffset()
    {
        TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();
    }

    /**
     * Convert the epoch days in UTC of a date (e.g., '1999-12-31') to the first millisecond in local time
     * of the date since the Unix epoch ('1970-01-01 00:00:00 UTC').
     * Leap seconds are not considered.
     */
    public static long utcDaysToLocalMillis(int day)
    {
        /**
         * Issue #419:
         * No need to add the timezone offset, because both days and milliseconds
         * are since the Unix epoch.
         */
        return Date.valueOf(LocalDate.ofEpochDay(day)).getTime();
    }

    /**
     * Convert the first millisecond in local time of a date (e.g., '1999-12-31') since the
     * Unix epoch ('1970-01-01 00:00:00 UTC') to the epoch days in UTC of the date.
     * Leap seconds are not considered.
     * <b>If the default timezone is changed, must call {@link #resetTimezoneOffset()} before this method.</b>
     */
    public static int localMillisToUtcDays(long millis)
    {
        return Math.round((millis + TIMEZONE_OFFSET) / 86400000f);
    }

    /**
     * Convert the {@link Date} in local time to the days in epoch day in UTC of the date (e.g., '1999-12-31').
     * This method produces more temporary objects than:<br/>
     * {@code millisToDay(date.getTime())}.
     */
    public static int sqlDateToDay (Date date)
    {
        return (int) date.toLocalDate().toEpochDay();
    }

    /**
     * Convert the {@link Date} to the days since the Unix epoch ('1970-01-01').
     * @param date
     * @return
     */
    public static int stringToDay (String date)
    {
        return (int) LocalDate.parse(date).toEpochDay();
    }

    public static long daysToMillis (int days)
    {
        return (long) days * 86400000L;
    }

    /**
     * Rounds the number of milliseconds relative to the epoch down to the nearest whole number of
     * seconds. 500 would round to 0, -500 would round to -1.
     */
    public static long millisToSeconds(long millis)
    {
        if (millis >= 0)
        {
            return millis / 1000;
        }
        else
        {
            return (millis - 999) / 1000;
        }
    }

    /**
     * Get the milliseconds in a day.
     * @param millis the milliseconds since the Unix epoch ('1970-01-01 00:00:00 UTC');
     * @return the milliseconds in a day.
     */
    public static int millisInDay(long millis)
    {
        return (int)(millis % 86400000);
    }

    /**
     * Convert a string representation of the time in the current local timezone to the
     * milliseconds since the start of the day in UTC time.
     * @param localTime the string representation of local time, in the format of HH:mm:ss[.SSS]
     * @return the milliseconds in the day
     */
    public static int parseTime(String localTime)
    {
        int hour;
        int minute;
        int second;
        int millis;
        int firstColon;
        int secondColon;
        int decimalPoint;

        if (localTime == null)
        {
            throw new IllegalArgumentException("the input string is null");
        }

        firstColon = localTime.indexOf(':');
        secondColon = localTime.indexOf(':', firstColon+1);
        decimalPoint = localTime.indexOf('.', secondColon + 1);
        if ((firstColon > 0) && (secondColon > 0) && (secondColon < s.length()-1))
        {
            hour = Integer.parseInt(localTime.substring(0, firstColon));
            minute = Integer.parseInt(localTime.substring(firstColon+1, secondColon));

            if (decimalPoint > 0 && decimalPoint < localTime.length()-1)
            {
                second = Integer.parseInt(localTime.substring(secondColon+1, decimalPoint));
                millis = Integer.parseInt(localTime.substring(decimalPoint+1));
            }
            else
            {
                second = Integer.parseInt(localTime.substring(secondColon+1));
                millis = 0;
            }
        } else {
            throw new java.lang.IllegalArgumentException();
        }

        return (int) ((hour * 3600000L + minute * 60000L + second * 1000L + millis - TIMEZONE_OFFSET) % 86400000);
    }
}
