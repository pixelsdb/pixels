/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.daemon.sink;

import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.ServiceLoader;

public class SinkServer implements Server
{
    private final SinkProvider sinkProvider;

    public SinkServer()
    {
        ServiceLoader<SinkProvider> sinkProviders = ServiceLoader.load(SinkProvider.class);
        for(SinkProvider sinkProvider: sinkProviders)
        {
            this.sinkProvider = sinkProvider;
            return;
        }
        sinkProvider = null;
    }

    @Override
    public boolean isRunning()
    {
        return sinkProvider.isRunning();
    }

    @Override
    public void shutdown()
    {
        sinkProvider.shutdown();
    }

    @Override
    public void run()
    {
        sinkProvider.start(ConfigFactory.Instance());
    }
}
