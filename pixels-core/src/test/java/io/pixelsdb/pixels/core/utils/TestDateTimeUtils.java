/*
 * Copyright 2022 PixelsDB.
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

import org.junit.Test;

import java.sql.Date;
import java.util.TimeZone;

/**
 * @author hank
 * @create 2023-04-18
 */
public class TestDateTimeUtils
{
    @Test
    public void testGetDaysSinceEpoch()
    {
        int day1, day2, day3;
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+12:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+14:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT-8:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT-12:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;

        TimeZone.setDefault(TimeZone.getTimeZone("GMT-14:00"));
        System.out.println(TimeZone.getDefault().getDisplayName() + ", offset=" +
                TimeZone.getDefault().getRawOffset());
        DatetimeUtils.resetTimezoneOffset();
        day1 = DatetimeUtils.sqlDateToDay(Date.valueOf("2023-03-05"));
        day2 = DatetimeUtils.stringToDay("2023-03-05");
        day3 = DatetimeUtils.millisToDay(Date.valueOf("2023-03-05").getTime());
        assert day1 == day2 && day2 == day3;
    }
}
