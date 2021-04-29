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

import java.util.Calendar;

/**
 * Created at: 26/04/2021
 * Author: hank
 */
public class DatetimeUtils
{
    // TODO: currently we assume there are 86400000 millis in a day, without dealing with leap second.
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

    public static int roundSqlTime (long millis)
    {
        return (int)(millis%86400000);
    }
}
