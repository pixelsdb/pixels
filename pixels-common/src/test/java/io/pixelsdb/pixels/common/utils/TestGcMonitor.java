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
package io.pixelsdb.pixels.common.utils;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

/**
 * @author hank
 * @date 04/08/2022
 */
public class TestGcMonitor
{
    @Test
    public void test()
    {
        GcMonitor.GcStats stats = GcMonitor.Instance().createGcStats();
        GcMonitor.Instance().doneGcStats(stats);
        System.out.println(stats.gcTimeProportion());
        for (int i = 0; i < 1000_000; ++i)
        {
            byte[] bytes = new byte[2000];
        }
        System.gc();
        GcMonitor.Instance().doneGcStats(stats);
        System.out.println(stats.gcTimeProportion());
        System.out.println(JSON.toJSONString(stats));
    }
}
