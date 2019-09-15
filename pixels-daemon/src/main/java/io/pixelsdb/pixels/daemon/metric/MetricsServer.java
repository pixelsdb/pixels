/*
 * Copyright 2018 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
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

/**
 * @author hank
 */
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
