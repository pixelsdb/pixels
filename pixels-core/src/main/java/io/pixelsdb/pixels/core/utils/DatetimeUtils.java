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

/**
 * @author hank
 * @create 2021-04-26
 */
public class DatetimeUtils
{
    // TODO: currently we assume there are 86400000 millis in a day, without dealing with leap second.

    public static long dayToMillis (int day)
    {
        /**
         * Issue #419:
         * In SQL, Date (day) does not have time zone, hence we do not need to add timezone offset.
         */
        return day * 86400000L;
    }

    /**
     * Convert the milliseconds to the days, both since the Unix epoch ('1970-01-01 00:00:00'),
     * without considering the effect of timezone.
     * @param millis the milliseconds since the Unix epoch.
     * @return the days since the Unix epoch.
     */
    public static int millisToDay (long millis)
    {
        return (int)((millis) / 86400000);
    }

    public static int roundSqlTime (long millis)
    {
        return (int)(millis%86400000);
    }
}
