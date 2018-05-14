package cn.edu.ruc.iir.pixels.daemon.metric;

import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.daemon.Server;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestMetricServer
{
    @Test
    public void test () throws InterruptedException
    {
        ConfigFactory.Instance().addProperty("metric.node.text.dir", "/home/hank/");
        Server server = new MetricsServer();
        Thread thread = new Thread(server);
        thread.start();
        TimeUnit.SECONDS.sleep(5);
        System.exit(0);
    }
}
