package io.pixelsdb.pixels.scaling;

import io.pixelsdb.pixels.common.transaction.TransContextCache;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsCollector extends io.pixelsdb.pixels.common.turbo.MetricsCollector {
    private final TransContextCache transContextCache;
    private final MonitorClient monitorClient;
    private final int period;
    private final ScheduledExecutorService metricsReporter;

    protected MetricsCollector() {
        // Starting a background thread to report query concurrency periodically.
        this.transContextCache = TransContextCache.Instance();
        this.monitorClient = new MonitorClient(ConfigFactory.Instance().getProperty("monitor.metrics.name"),
                Integer.parseInt(ConfigFactory.Instance().getProperty("monitor.server.port")));
        this.metricsReporter = Executors.newScheduledThreadPool(1);
        this.period = Integer.parseInt(ConfigFactory.Instance().getProperty("query.concurrency.report.period.sec"));
    }

    @Override
    public void startAutoReport() {
        this.metricsReporter.scheduleAtFixedRate(() -> {
            monitorClient.reportMetric(transContextCache.getQueryConcurrency());
        }, 0, period, TimeUnit.SECONDS);
    }

    @Override
    public void report() {
        int concurrency = this.transContextCache.getQueryConcurrency();
        this.monitorClient.reportMetric(concurrency);
    }

    @Override
    public void stopAutoReport() {
        this.metricsReporter.shutdown();
        this.metricsReporter.shutdownNow();
        this.monitorClient.stopReportMetric();
    }
}
