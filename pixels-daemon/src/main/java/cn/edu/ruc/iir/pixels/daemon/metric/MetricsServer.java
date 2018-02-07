package cn.edu.ruc.iir.pixels.daemon.metric;

import cn.edu.ruc.iir.pixels.common.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.Server;

import java.util.concurrent.TimeUnit;

public class MetricsServer implements Server
{
    private boolean running = false;

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public void shutdown()
    {
        this.running = false;
    }

    @Override
    public void run()
    {
        this.running = true;
        while (true)
        {
            try
            {
                // parse the json files under /dev/shm/pixels/
                // calculate the histogram.
                // save it as prom file under the text file dir of prometheus node exporter.
                TimeUnit.SECONDS.sleep(600);
            } catch (InterruptedException e)
            {
                LogFactory.Instance().getLog().error("interrupted in main loop of metrics server.", e);
            }
        }
    }
}
