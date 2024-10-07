package io.pixelsdb.pixels.daemon.monitor;

import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestMonitorServer {
    MonitorServer server;
    MockClient client;

    @Test
    public void testReportOneMetric() {
        server = new MonitorServer(54333);
        new Thread(() -> server.run()).start();
        // wait monitor server to start
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client = new MockClient(54333);
        client.reportMetric(10);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testReportMetrics() {
        MonitorServer server = new MonitorServer(54333);
        new Thread(server).start();
        // wait monitor server to start
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client = new MockClient(54333);

        ArrayList<Integer> metrics = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5));
        Random random = new Random();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            client.reportMetric(metrics.get(random.nextInt(metrics.size())));
        }, 0, 3, TimeUnit.SECONDS);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void close() {
        if (client != null) {
            client.stopReportMetric();
        }
        if (server != null) {
            server.shutdown();
        }
    }
}
