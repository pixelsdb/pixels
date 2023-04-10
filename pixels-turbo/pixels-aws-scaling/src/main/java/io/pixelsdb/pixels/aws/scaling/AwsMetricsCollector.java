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
package io.pixelsdb.pixels.aws.scaling;

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
public class AwsMetricsCollector
{
    private static final AwsMetricsCollector instance = new AwsMetricsCollector();

    public static AwsMetricsCollector Instance()
    {
        return instance;
    }

    private final TransContext transContext;
    private final CloudWatchMetrics cloudWatchMetrics;
    private final String metricsName;
    private final int period;
    private final ScheduledExecutorService metricsReporter;

    private AwsMetricsCollector()
    {
        // Starting a background thread to report query concurrency periodically.
        this.transContext = TransContext.Instance();
        this.cloudWatchMetrics = new CloudWatchMetrics();
        this.metricsName = ConfigFactory.Instance().getProperty("query.concurrency.metrics.name");
        this.metricsReporter = Executors.newScheduledThreadPool(1);
        this.period = Integer.parseInt(ConfigFactory.Instance().getProperty("query.concurrency.report.period.sec"));
    }

    public void startAutoReport()
    {
        this.metricsReporter.scheduleAtFixedRate(() -> {
            NamedCount count = new NamedCount(metricsName, transContext.getTransConcurrency());
            cloudWatchMetrics.putCount(count);
        }, 0, period, TimeUnit.SECONDS);
    }

    public void report()
    {
        int concurrency = this.transContext.getTransConcurrency();
        NamedCount count = new NamedCount(this.metricsName, concurrency);
        this.cloudWatchMetrics.putCount(count);
    }

    public void stopAutoReport()
    {
        this.metricsReporter.shutdown();
        this.metricsReporter.shutdownNow();
    }
}
