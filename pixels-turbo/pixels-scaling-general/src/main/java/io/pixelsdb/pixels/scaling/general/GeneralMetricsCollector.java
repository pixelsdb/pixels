/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.scaling.general;

import io.pixelsdb.pixels.common.transaction.TransContextCache;
import io.pixelsdb.pixels.common.turbo.MetricsCollector;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GeneralMetricsCollector extends MetricsCollector
{
    private final TransContextCache transContextCache;
    private final ScalingMetricsClient scalingMetricsClient;
    private final int period;
    private final ScheduledExecutorService metricsReporter;

    protected GeneralMetricsCollector()
    {
        // Starting a background thread to report query concurrency periodically.
        this.transContextCache = TransContextCache.Instance();
        this.scalingMetricsClient = new ScalingMetricsClient(ConfigFactory.Instance().getProperty("scaling.metrics.name"),
                Integer.parseInt(ConfigFactory.Instance().getProperty("scaling.metrics.server.port")));
        this.metricsReporter = Executors.newScheduledThreadPool(1);
        this.period = Integer.parseInt(ConfigFactory.Instance().getProperty("query.concurrency.report.period.sec"));
    }

    @Override
    public void startAutoReport()
    {
        this.metricsReporter.scheduleAtFixedRate(() -> {
            scalingMetricsClient.reportMetric(transContextCache.getQueryConcurrency());
        }, 0, period, TimeUnit.SECONDS);
    }

    @Override
    public void report()
    {
        int concurrency = this.transContextCache.getQueryConcurrency();
        this.scalingMetricsClient.reportMetric(concurrency);
    }

    @Override
    public void stopAutoReport()
    {
        this.metricsReporter.shutdown();
        this.metricsReporter.shutdownNow();
        this.scalingMetricsClient.stopReportMetric();
    }
}