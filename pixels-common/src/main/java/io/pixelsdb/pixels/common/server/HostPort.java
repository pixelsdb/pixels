/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.common.server;

import java.util.Objects;

/**
 * @author hank
 * @create 2024-09-16
 */
public class HostPort
{
    private final String host;
    private final int port;

    public HostPort(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.host, this.port);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof HostPort))
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        HostPort that = (HostPort) obj;
        return Objects.equals(this.host, that.host) && this.port == that.port;
    }

    public boolean equals(String host, int port)
    {
        return Objects.equals(this.host, host) && this.port == port;
    }

    @Override
    public String toString()
    {
        return this.host + ":" + this.port;
    }
}
