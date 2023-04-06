/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.planner.autoscaling;

import io.pixelsdb.pixels.common.metrics.CloudWatchCountMetrics;
import io.pixelsdb.pixels.common.metrics.NamedCount;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created at: 19/01/2023
 * Author: hank
 */
public class MetricsCollector
{
    private static final MetricsCollector instance = new MetricsCollector();

    public static MetricsCollector Instance()
    {
        return instance;
    }

    private final TransContext transContext;
    private final CloudWatchCountMetrics cloudWatchCountMetrics;
    private final String metricsName;
    private final int period;
    private final ScheduledExecutorService metricsReporter;

    private MetricsCollector()
    {
        // Starting a background thread to report query concurrency periodically.
        this.transContext = TransContext.Instance();
        this.cloudWatchCountMetrics = new CloudWatchCountMetrics();
        this.metricsName = ConfigFactory.Instance().getProperty("query.concurrency.metrics.name");
        this.metricsReporter = Executors.newScheduledThreadPool(1);
        this.period = Integer.parseInt(ConfigFactory.Instance().getProperty("query.concurrency.report.period.sec"));
    }

    public void startAutoReport()
    {
        this.metricsReporter.scheduleAtFixedRate(() -> {
            NamedCount count = new NamedCount(metricsName, transContext.getTransConcurrency());
            cloudWatchCountMetrics.putCount(count);
        }, 0, period, TimeUnit.SECONDS);
    }

    public void report()
    {
        int concurrency = this.transContext.getTransConcurrency();
        NamedCount count = new NamedCount(this.metricsName, concurrency);
        this.cloudWatchCountMetrics.putCount(count);
    }

    public void stopAutoReport()
    {
        this.metricsReporter.shutdown();
        this.metricsReporter.shutdownNow();
    }
}
