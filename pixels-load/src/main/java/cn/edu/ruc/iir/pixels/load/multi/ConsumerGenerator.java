package cn.edu.ruc.iir.pixels.load.multi;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
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

    public boolean startConsumer(BlockingQueue<Path> queue, Config config)
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
        } catch (IOException e)
        {
            e.printStackTrace();
        } catch (MetadataException e)
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