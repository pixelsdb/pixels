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
package io.pixelsdb.pixels.invoker.vhive;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

public class Vhive
{
    private static final ConfigFactory config = ConfigFactory.Instance();
    private static final Vhive instance = new Vhive();
    private final WorkerAsyncClient asyncClient;

    private Vhive()
    {
        String hostname = config.getProperty("vhive.hostname");
        int port = Integer.parseInt(config.getProperty("vhive.port"));
        asyncClient = new WorkerAsyncClient(hostname, port);
    }

    public static Vhive Instance()
    {
        return instance;
    }

    public WorkerAsyncClient getAsyncClient()
    {
        return asyncClient;
    }
}
