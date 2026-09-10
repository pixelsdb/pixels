/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public License
 * along with Pixels.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Network endpoint used by runtime workers to call back to the coordinator.
 */
public class CoordinatorEndpoint
{
    private final String host;
    private final int port;

    public CoordinatorEndpoint(String host, int port)
    {
        this.host = requireNonNull(host, "host is null").trim();
        checkArgument(!this.host.isEmpty(), "host is empty");
        checkArgument(port > 0 && port <= 65535, "port is out of range");
        this.port = port;
    }

    public static CoordinatorEndpoint fromConfig()
    {
        ConfigFactory config = ConfigFactory.Instance();
        String host = config.getProperty("worker.coordinate.server.host");
        String port = config.getProperty("worker.coordinate.server.port");
        checkArgument(port != null && !port.trim().isEmpty(),
                "worker.coordinate.server.port is empty");
        return new CoordinatorEndpoint(host, Integer.parseInt(port));
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }
}
