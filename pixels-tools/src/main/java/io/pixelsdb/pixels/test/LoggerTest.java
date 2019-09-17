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
package io.pixelsdb.pixels.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * pixels
 *
 * @author guodong
 */
public class LoggerTest
{
    public static Logger logger = LogManager.getLogger(LoggerTest.class);

    public static void main(String[] args)
    {
        Thread[] threads = new Thread[20];
        long start = System.nanoTime();
        for (int i = 0; i < 20; i++)
        {
            threads[i] = new Thread(new LogThread(String.valueOf(i)));
            threads[i].start();
        }
        for (int i = 0; i < 20; i++)
        {
            try
            {
                threads[i].join();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        long end = System.nanoTime();
        logger.info("Total cost: " + (end - start));
    }

    static class LogThread implements Runnable
    {
        private final String id;

        public LogThread(String id)
        {
            this.id = id;
        }

        @Override
        public void run()
        {
            long sum = 0;
            long begin = System.nanoTime();
            for (int i = 0; i < 2000; i++)
            {
                sum += i;
//                logger.debug("thread " + id + ", round: " + i);
            }
            long end = System.nanoTime();
            logger.debug("[done] thread " + id + " : " + (end - begin) + ". " + sum);
        }
    }
}
