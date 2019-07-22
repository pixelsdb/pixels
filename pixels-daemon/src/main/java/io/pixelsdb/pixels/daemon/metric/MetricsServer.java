package io.pixelsdb.pixels.daemon.metric;

import io.pixelsdb.pixels.common.metrics.ReadPerfHistogram;
import io.pixelsdb.pixels.common.metrics.ReadPerfMetrics;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.Server;
import com.alibaba.fastjson.JSON;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MetricsServer implements Server
{
    private static Logger log = LogManager.getLogger(MetricsServer.class);

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
                        log.error("I/O exception when reading metrics from json.", e);
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
                log.error("interrupted in main loop of metrics server.", e);
            } catch (IOException e)
            {
                log.error("I/O error in main loop of metrics server.", e);
            }
        }
    }
}
