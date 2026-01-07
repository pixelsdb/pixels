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
package io.pixelsdb.pixels.common.index.service;

/**
 * Factory class for creating IndexService instances.
 * It chooses between RPCIndexService and LocalIndexService
 * according to the configuration.
 */
public class IndexServiceProvider
{
    public static IndexService getService(ServiceMode mode)
    {
        switch (mode)
        {
            case rpc:
                return RPCIndexService.Instance();
            case local:
                return LocalIndexService.Instance();
            default:
                throw new IllegalStateException("Unexpected service mode: " + mode);
        }
    }

    public enum ServiceMode
    {
        rpc, local;

        public static ServiceMode from(String name)
        {
            for (ServiceMode mode : values())
            {
                if (mode.name().equalsIgnoreCase(name))
                {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unsupported IndexService mode: " + name);
        }
    }
}

