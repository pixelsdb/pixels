package io.pixelsdb.pixels.daemon.monitor.util;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.monitor.MetricsQueue;
import io.pixelsdb.pixels.daemon.monitor.policy.BasicPolicy;
import io.pixelsdb.pixels.daemon.monitor.policy.PidPolicy;
import io.pixelsdb.pixels.daemon.monitor.policy.Policy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PolicyManager implements Runnable {
    private static final Logger log = LogManager
            .getLogger(PolicyManager.class);
    private final Policy policy;

    public PolicyManager() {
        ConfigFactory config = ConfigFactory.Instance();
        String policyName = config.getProperty("vm.auto.scaling.policy");
        switch (policyName) {
            case "pid":
                policy = new PidPolicy();
                break;
            default:
                policy = new BasicPolicy();
                break;
        }
    }

    @Override
    public void run() {
        while (true) {
            log.debug("Debug: Receive metrics:" + MetricsQueue.queue);
            doAutoScaling();
        }
    }

    public void doAutoScaling() {
        policy.doAutoScaling();
    }
}
