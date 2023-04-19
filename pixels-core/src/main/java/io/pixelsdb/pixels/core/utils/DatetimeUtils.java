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
     * Convert the days to milliseconds, both since the Unix epoch ('1970-01-01 00:00:00 UTC').
     * Leap seconds are not considered.
     */
    public static long dayToMillis (int day)
    {
        /**
         * Issue #419:
         * No need to add the timezone offset, because both days and milliseconds
         * are since the Unix epoch.
         * TODO: add leap seconds.
         */
        return day * 86400000L;
    }

    /**
     * Convert the milliseconds to days, both since the Unix epoch ('1970-01-01 00:00:00 UTC').
     * Leap seconds are not considered.
     * <b>If the default timezone is changed, must call {@link #resetTimezoneOffset()} before this method.</b>
     */
    public static int millisToDay (long millis)
    {
        // TODO: add leap seconds.
        return Math.round((millis + TIMEZONE_OFFSET) / 86400000f);
    }

    /**
     * Convert the {@link Date} of local time to the days since the Unix epoch ('1970-01-01 00:00:00 UTC').
     * This method produces more temporary objects than:<br/>
     * {@code millisToDay(date.getTime())}.
     */
    public static int sqlDateToDay (Date date)
    {
        return (int) date.toLocalDate().toEpochDay();
    }

    /**
     * Convert the {@link Date} of local time to the days since the Unix epoch ('1970-01-01 00:00:00 UTC').
     * @param date
     * @return
     */
    public static int stringToDay (String date)
    {
        return (int) LocalDate.parse(date).toEpochDay();
    }

    public static int roundSqlTime (long millis)
    {
        return (int)(millis % 86400000);
    }
}
