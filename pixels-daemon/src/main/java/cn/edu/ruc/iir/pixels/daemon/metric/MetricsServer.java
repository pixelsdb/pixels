package cn.edu.ruc.iir.pixels.daemon.metric;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.Server;
import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
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
                String jsonDir = ConfigFactory.Instance().getProperty("metric.reader.json.dir");
                File dir = new File(jsonDir);
                File[] jsonFiles = dir.listFiles((file, s) -> s.endsWith(".json"));
                List<ReadPerfMetrics> metricsList = new ArrayList<>();
                for (File jsonFile : jsonFiles)
                {
                    try (BufferedReader reader = new BufferedReader(new FileReader(jsonFile)))
                    {
                        String line;
                        StringBuilder jsonStr = new StringBuilder();
                        while ((line = reader.readLine()) != null)
                        {
                            jsonStr.append(line);
                        }
                        ReadPerfMetrics metrics = JSON.parseObject(jsonStr.toString(), ReadPerfMetrics.class);
                        metricsList.add(metrics);
                    } catch (java.io.IOException e)
                    {
                        LogFactory.Instance().getLog().error("I/O exception when reading metrics from json.", e);
                    }
                }

                // calculate the histogram.
                // save it as prom file under the text file dir of prometheus node exporter.
                String textDir = ConfigFactory.Instance().getProperty("metric.node.text.dir");
                TimeUnit.SECONDS.sleep(600);
            } catch (InterruptedException e)
            {
                LogFactory.Instance().getLog().error("interrupted in main loop of metrics server.", e);
            }
        }
    }
}
