package io.pixelsdb.pixels.daemon.monitor.policy;

public class BasicPolicy extends Policy {
    private static final int UPPER_BOUND = 3;
    private static final int LOWER_BOUND = 2;

    @Override
    public void doAutoScaling() {
        try {
            int queryConcurrency = metricsQueue.take();
            if (queryConcurrency < LOWER_BOUND) {
                System.out.println("Debug: reduce one vm");
                scalingManager.reduceOne();
            } else if (queryConcurrency > UPPER_BOUND) {
                System.out.println("Debug: expand one vm");
                scalingManager.expandOne();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
