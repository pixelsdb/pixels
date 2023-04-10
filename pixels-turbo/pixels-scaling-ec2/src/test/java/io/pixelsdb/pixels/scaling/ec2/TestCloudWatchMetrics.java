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
package io.pixelsdb.pixels.scaling.ec2;

import io.pixelsdb.pixels.common.metrics.NamedCount;
import org.junit.Test;

/**
 * Created at: 29/12/2022
 * Author: hank
 */
public class TestCloudWatchMetrics
{
    @Test
    public void testMultiple() throws InterruptedException
    {
        int[] concurrency = new int[] {1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
        //int[] concurrency = new int[] {3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 3, 1, 1, 1, 1, 1, 3, 3, 1};
        CloudWatchMetrics metrics = new CloudWatchMetrics();
        for (int i = 0; i < 21; ++i)
        {
            int finalI = i;
            Thread thread = new Thread(() -> metrics.putCount(new NamedCount("query-concurrency", concurrency[finalI])));
            thread.start();
            Thread.sleep(10000);
        }
    }

    @Test
    public void testSingle()
    {
        CloudWatchMetrics metrics = new CloudWatchMetrics();
        metrics.putCount(new NamedCount("query-concurrency", 1));
        metrics.putCount(new NamedCount("query-concurrency", 2));
    }
}
