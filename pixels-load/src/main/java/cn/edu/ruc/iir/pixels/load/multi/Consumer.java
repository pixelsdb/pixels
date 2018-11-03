package cn.edu.ruc.iir.pixels.load.multi;

import org.apache.hadoop.fs.Path;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public abstract class Consumer extends Thread
{

    protected Consumer()
    {
    }

    private BlockingQueue<Path> queue;
    private Properties prop;
    private Config config;

    public Properties getProp()
    {
        return prop;
    }

    public Consumer(BlockingQueue<Path> queue, Properties prop, Config config)
    {
        this.queue = queue;
        this.prop = prop;
        this.config = config;
    }

}
