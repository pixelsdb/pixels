package cn.edu.ruc.iir.pixels.daemon.metric;

import cn.edu.ruc.iir.pixels.common.ConfigFactory;
import cn.edu.ruc.iir.pixels.common.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.Server;
import com.alibaba.fastjson.JSON;

import java.io.*;
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
        while (this.running)
        {
            try
            {
                // parse the json files under /dev/shm/pixels/
                // and calculate the histogram.
                String jsonDir = ConfigFactory.Instance().getProperty("metric.reader.json.dir");
                File dir = new File(jsonDir);
                File[] jsonFiles = dir.listFiles((file, s) -> s.endsWith(".json"));
                ReadPerfHistogram histogram = new ReadPerfHistogram();
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
                        histogram.addMetrics(metrics);
                    } catch (java.io.IOException e)
                    {
                        LogFactory.Instance().getLog().error("I/O exception when reading metrics from json.", e);
                    }
                }

                // save it as prom file under the text file dir of prometheus node exporter.
                String textDir = ConfigFactory.Instance().getProperty("metric.node.text.dir");
                if (!textDir.endsWith("/"))
                {
                    textDir += "/";
                }

                File textFile = new File(textDir + "node-perf-metrics.prom");
                if (textFile.exists())
                {
                    textFile.delete();
                }
                BufferedWriter writer = new BufferedWriter(new FileWriter(textFile));
                String text = histogram.toPromTextFormat();
                writer.write(text);
                writer.close();
                TimeUnit.SECONDS.sleep(60);
            } catch (InterruptedException e)
            {
                LogFactory.Instance().getLog().error("interrupted in main loop of metrics server.", e);
            } catch (IOException e)
            {
                LogFactory.Instance().getLog().error("I/O error in main loop of metrics server.", e);
            }
        }
    }
}
