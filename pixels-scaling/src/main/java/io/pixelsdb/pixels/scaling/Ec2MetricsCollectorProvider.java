package io.pixelsdb.pixels.scaling;

import io.pixelsdb.pixels.common.turbo.MachineService;
import io.pixelsdb.pixels.common.turbo.MetricsCollector;
import io.pixelsdb.pixels.common.turbo.MetricsCollectorProvider;

public class Ec2MetricsCollectorProvider implements MetricsCollectorProvider {
    @Override
    public MetricsCollector createMetricsCollector() {
        return new Ec2MetricsCollector();
    }

    @Override
    public boolean compatibleWith(MachineService machineService) {
        return machineService.equals(MachineService.ec2);
    }
}
