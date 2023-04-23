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

/**
 * The SPI for the metrics collector provider of the virtual machine service.
 * In pixels.properties, set scaling.machine.service to the virtual machine service that the metrics collector
 * implementation is compatible with.
 * In Pixels, there should be at most one metrics collector implementation compatible with each virtual machine service.
 *
 * @create 2023-04-10
 * @author hank
 */
public interface MetricsCollectorProvider
{
    /**
     * Create an instance of the configured metrics collector.
     */
    MetricsCollector createMetricsCollector();

    /**
     * @param machineService the given cloud function service.
     * @return true if this metrics collector provider is compatible with the given virtual machine service.
     */
    boolean compatibleWith(MachineService machineService);
}
