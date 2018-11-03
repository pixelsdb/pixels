package cn.edu.ruc.iir.pixels.load.multi;

import cn.edu.ruc.iir.pixels.common.utils.DateUtil;
import org.apache.hadoop.fs.Path;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class ORCConsumer
        extends Consumer
{
    private BlockingQueue<Path> queue;
    private Properties prop;
    private Config config;

    public Properties getProp()
    {
        return prop;
    }

    public ORCConsumer(BlockingQueue<Path> queue, Properties prop, Config config)
    {
        this.queue = queue;
        this.prop = prop;
        this.config = config;
    }

    private enum VECTOR_CLAZZ
    {
        BytesColumnVector, DoubleColumnVector, LongColumnVector
    }

    // todo fill the runninr part of ORCConsumer
    @Override
    public void run()
    {
        System.out.println("Start PixelsConsumer, " + Thread.currentThread().getName() + ", time: " + DateUtil.formatTime(new Date()));
        boolean isRunning = true;

        while (isRunning)
        {

        }
    }
}