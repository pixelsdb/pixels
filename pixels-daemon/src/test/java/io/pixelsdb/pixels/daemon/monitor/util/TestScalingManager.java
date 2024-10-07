package io.pixelsdb.pixels.daemon.monitor.util;

import org.junit.Test;

public class TestScalingManager {
    @Test
    public void testExpandSome() {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.expandSome(3);
    }

    @Test
    public void testReduceSome() {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.reduceSome(3);
    }

    @Test
    public void testReduceAll() {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.reduceAll();
    }

    @Test
    public void testMultiplyInstance() {
        ScalingManager scalingManager = new ScalingManager();
        scalingManager.multiplyInstance(2);
        scalingManager.multiplyInstance(0.75f);
    }
}
