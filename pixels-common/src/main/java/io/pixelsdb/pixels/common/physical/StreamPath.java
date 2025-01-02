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
package io.pixelsdb.pixels.common.physical;

import static java.util.Objects.requireNonNull;

public class StreamPath
{
    private String host;
    private int port;
    public boolean valid = false;

    public StreamPath(String path)
    {
        requireNonNull(path);
        if (path.contains("://"))
        {
            path = path.substring(path.indexOf("://") + 3);
        }
        int colon = path.indexOf(':');
        if (colon > 0)
        {
            host = path.substring(0, colon);
            port = Integer.parseInt(path.substring(colon + 1));
            this.valid = true;
        }
    }

    public String getHostName()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

}
