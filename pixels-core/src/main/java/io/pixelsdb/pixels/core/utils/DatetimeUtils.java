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
import java.time.LocalDate;
import java.util.TimeZone;

/**
 * @author hank
 * @create 2021-04-26
 */
public class DatetimeUtils
{
    private static long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();

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
}
