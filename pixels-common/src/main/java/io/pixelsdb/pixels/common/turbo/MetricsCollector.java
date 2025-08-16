/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.turbo;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Optional;
import java.util.ServiceLoader;

/**
 * {@link MetricsCollector} collects the performance metrics (e.g., query concurrency, CPU, or memory) in the workers
 * of the {@link MachineService}. The collected metrics are used to monitor and auto-scaling the machine service.
 * @create 2023-04-10
 * @author hank
 */
public abstract class MetricsCollector
{
    private static MetricsCollector instance;

    static
    {
        String serviceName = ConfigFactory.Instance().getProperty("scaling.machine.service");
        MachineService machineService = MachineService.from(serviceName);
        ServiceLoader<MetricsCollectorProvider> providerLoader = ServiceLoader.load(MetricsCollectorProvider.class);
        providerLoader.forEach(provider -> {
            if (provider.compatibleWith(machineService))
            {
                instance = provider.createMetricsCollector();
            }
        });
    }

    /**
     * @return the instance of the configured metrics collector implementation.
     */
    public static Optional<MetricsCollector> Instance()
    {
        return Optional.ofNullable(instance);
    }

    protected MetricsCollector() { }

    /**
     * Start automatic metrics reporting.
     */
    abstract public void startAutoReport();

    /**
     * Manually report metrics.
     */
    abstract public void report();

    /**
     * Stop automatic metrics reporting.
     */
    abstract public void stopAutoReport();
}
