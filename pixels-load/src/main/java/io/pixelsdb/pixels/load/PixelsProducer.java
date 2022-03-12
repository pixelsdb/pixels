/*
 * Copyright 2018-2019 PixelsDB.
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
package io.pixelsdb.pixels.load;

import org.apache.hadoop.fs.Path;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Description: souce -> BlockingQueue
 * @author: tao
 * @author hank 
 * @date: Create in 2018-10-30 12:57
 **/
public class PixelsProducer
{
    private static PixelsProducer instance = new PixelsProducer();

    public PixelsProducer()
    {
    }

    public static PixelsProducer getInstance()
    {
        return instance;
    }

    public void startProducer(BlockingQueue<Path> queue)
    {
    }

    class Producer extends Thread
    {

        private volatile boolean isRunning = true;
        private BlockingQueue<Path> queue;
        Path data = null;

        public Producer(BlockingQueue<Path> queue)
        {
            this.queue = queue;
        }

        @Override
        public void run()
        {
            System.out.println("start producer thread！");
            try
            {
                while (isRunning)
                {
                    System.out.println("begin to generate data...");

                    System.out.println("add：" + data + "into queue...");
                    if (!queue.offer(data, 2, TimeUnit.SECONDS))
                    {
                        System.out.println("add error：" + data);
                    }
                }
            } catch (InterruptedException e)
            {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            } finally
            {
                System.out.println("Exit producer thread！");
            }
        }

        public void stopProducer()
        {
            isRunning = false;
        }
    }

}
