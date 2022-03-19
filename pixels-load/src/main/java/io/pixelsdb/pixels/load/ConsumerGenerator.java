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

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * @Description: source -> pixels file
 * @author: tao
 * @date: Create in 2018-10-30 11:59
 **/
public class ConsumerGenerator
{

    // the number of thread
    private int threadNum;

    private static ConsumerGenerator instance = new ConsumerGenerator();

    public ConsumerGenerator()
    {
    }

    public static ConsumerGenerator getInstance(int threadNum)
    {
        instance.threadNum = threadNum;
        return instance;
    }

    public boolean startConsumer(BlockingQueue<String> queue, Config config)
    {
        // init info
        ConfigFactory configFactory = ConfigFactory.Instance();
        Properties prop = new Properties();
        prop.setProperty("pixel.stride", configFactory.getProperty("pixel.stride"));
        prop.setProperty("row.group.size", configFactory.getProperty("row.group.size"));
        prop.setProperty("block.size", configFactory.getProperty("block.size"));
        prop.setProperty("block.replication", configFactory.getProperty("block.replication"));

        boolean option = false;
        try
        {
            // load some config info
            option = config.load(configFactory);
        } catch (MetadataException e)
        {
            e.printStackTrace();
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        boolean flag = false;
        if (option)
        {
            Consumer[] consumers = new Consumer[threadNum];
            try
            {
                for (int i = 0; i < threadNum; i++)
                {
                    if (config.getFormat().equalsIgnoreCase("pixels"))
                    {
                        PixelsConsumer pixelsConsumer = new PixelsConsumer(queue, prop, config);
                        consumers[i] = pixelsConsumer;
                        pixelsConsumer.start();
                    } else if (config.getFormat().equalsIgnoreCase("orc"))
                    {
                        ORCConsumer orcConsumer = new ORCConsumer(queue, prop, config);
                        consumers[i] = orcConsumer;
                        orcConsumer.start();
                    }
                }
                for (Consumer c : consumers)
                {
                    try
                    {
                        c.join();
                    } catch (InterruptedException e)
                    {
                        throw new Exception("ConsumerGenerator InterruptedException, " + e.getMessage());
                    }
                }
                flag = true;
            } catch (Exception e)
            {
                try
                {
                    throw new Exception("ConsumerGenerator Error, " + e.getMessage());
                } catch (Exception e1)
                {
                    e1.printStackTrace();
                }
            }
        } else
        {
            System.out.println("Config loader is error.");
        }
        return flag;
    }

}