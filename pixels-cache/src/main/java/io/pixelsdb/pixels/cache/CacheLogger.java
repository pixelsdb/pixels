/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created at: 19-5-12
 * @author hank
 */
public class CacheLogger implements Runnable
{
    private static final Logger logger = LogManager.getLogger(CacheLogger.class);

    private ConcurrentLinkedQueue<Long> searchLatency = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<Long> readLatency = new ConcurrentLinkedQueue<>();
    private volatile boolean shutdown = false;

    public void setShutdown()
    {
        this.shutdown = true;
    }

    public void addSearchLatency(long latency)
    {
        this.searchLatency.add(latency);
    }

    public void addReadLatency(long latency)
    {
        this.readLatency.add(latency);
    }

    @Override
    public void run()
    {
        while (this.shutdown == false)
        {
            try
            {
                Thread.sleep(5000);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            Object[] searches = searchLatency.toArray();
            searchLatency.clear();
            Object[] reads = readLatency.toArray();
            readLatency.clear();

            long sum = 0;
            for (Object latency : searches)
            {
                sum += (Long) latency;
            }
            logger.info("avg search latency: (" + (sum * 1.0 / searches.length) + ") ns");

            sum = 0;
            for (Object latency : reads)
            {
                sum += (Long) latency;
            }
            logger.info("avg read latency: (" + (sum * 1.0 / reads.length) + ") ns");

        }
    }

    public static void main(String[] args) throws InterruptedException
    {
        CacheLogger cacheLogger = new CacheLogger();
        Thread thread = new Thread(cacheLogger);
        thread.start();
        Random random = new Random();

        for (int i = 0; i < 1000; ++i)
        {
            cacheLogger.addReadLatency(random.nextInt(100));
            cacheLogger.addSearchLatency(random.nextInt(200));
            Thread.sleep(100);
        }

        cacheLogger.setShutdown();
    }
}
