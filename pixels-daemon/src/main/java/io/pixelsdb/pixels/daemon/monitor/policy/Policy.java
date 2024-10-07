package io.pixelsdb.pixels.daemon.monitor.policy;

import io.pixelsdb.pixels.daemon.monitor.MetricsQueue;
import io.pixelsdb.pixels.daemon.monitor.util.ScalingManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingDeque;

public abstract class Policy {
    private static final Logger log = LogManager.getLogger(Policy.class);
    static BlockingDeque<Integer> metricsQueue;
    ScalingManager scalingManager;

    public Policy() {
        metricsQueue = MetricsQueue.queue;
        scalingManager = new ScalingManager();
    }

    public abstract void doAutoScaling();

}
