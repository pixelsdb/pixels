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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

/**
 * @author hank
 * @date 04/08/2022
 */
public class GcMonitor
{
    private static final GcMonitor instance = new GcMonitor();

    public static GcMonitor Instance()
    {
        return instance;
    }

    private GcMonitor() { }

    public static class GcStats
    {
        private long startTimeMs;
        private long startGcTimeMs;
        private long endTimeMs;
        private long endGcTimeMs;

        public long getStartTimeMs()
        {
            return startTimeMs;
        }

        public void setStartTimeMs(long startTimeMs)
        {
            this.startTimeMs = startTimeMs;
        }

        public long getStartGcTimeMs()
        {
            return startGcTimeMs;
        }

        public void setStartGcTimeMs(long startGcTimeMs)
        {
            this.startGcTimeMs = startGcTimeMs;
        }

        public long getEndTimeMs()
        {
            return endTimeMs;
        }

        public void setEndTimeMs(long endTimeMs)
        {
            this.endTimeMs = endTimeMs;
        }

        public long getEndGcTimeMs()
        {
            return endGcTimeMs;
        }

        public void setEndGcTimeMs(long endGcTimeMs)
        {
            this.endGcTimeMs = endGcTimeMs;
        }

        public double gcTimeProportion()
        {
            return (this.endGcTimeMs - this.startGcTimeMs) / (double) (this.endTimeMs - this.startTimeMs);
        }
    }

    /**
     * Create a gc statistics and begin collecting gc time.
     * @return the gc statistics
     */
    public GcStats createGcStats()
    {
        long gcTimeMs = 0L;
        GcStats stats = new GcStats();
        stats.setStartTimeMs(System.currentTimeMillis());
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans())
        {
            gcTimeMs += gcBean.getCollectionTime();
        }
        stats.setStartGcTimeMs(gcTimeMs);
        return stats;
    }

    /**
     * Finish collecting gc time of the gc statistics.
     */
    public GcStats doneGcStats(GcStats stats)
    {
        long gcTimeMs = 0L;
        stats.setEndTimeMs(System.currentTimeMillis());
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans())
        {
            gcTimeMs += gcBean.getCollectionTime();
        }
        stats.setEndGcTimeMs(gcTimeMs);
        return stats;
    }
}
