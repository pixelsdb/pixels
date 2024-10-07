package io.pixelsdb.pixels.daemon.monitor.policy;


public class PidPolicy extends Policy {
    @Override
    public void doAutoScaling() {
        System.out.println("TODO: pid policy");
        System.out.println("Receive metrics:" + metricsQueue);
    }
}
