package io.pixelsdb.pixels.scaling;

import io.pixelsdb.pixels.common.turbo.MachineService;

public class MetricsCollectorProvider implements io.pixelsdb.pixels.common.turbo.MetricsCollectorProvider {
    @Override
    public io.pixelsdb.pixels.common.turbo.MetricsCollector createMetricsCollector() {
        return new MetricsCollector();
    }

    @Override
    public boolean compatibleWith(MachineService machineService) {
        return machineService.equals(MachineService.ec2);
    }
}
