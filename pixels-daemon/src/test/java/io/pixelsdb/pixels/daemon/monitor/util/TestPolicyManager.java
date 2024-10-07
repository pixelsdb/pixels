package io.pixelsdb.pixels.daemon.monitor.util;

import io.pixelsdb.pixels.daemon.monitor.MetricsQueue;
import org.junit.Test;

import java.util.concurrent.BlockingDeque;

public class TestPolicyManager {
    @Test
    public void testDoAutoScalingMethod() {
        PolicyManager policyManager = new PolicyManager();
        BlockingDeque<Integer> metricsQueue = MetricsQueue.queue;
        metricsQueue.add(5);
        policyManager.doAutoScaling();
        metricsQueue.add(1);
        policyManager.doAutoScaling();
    }

    @Test
    public void testConsumer() {
        new Thread(new PolicyManager()).start();
        BlockingDeque<Integer> metricsQueue = MetricsQueue.queue;
        metricsQueue.add(5);
        metricsQueue.add(1);
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
