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
package io.pixelsdb.pixels.daemon.heartbeat;

/**
 * @author hank
 * @create 2024-06-17
 */
public enum WorkerStatus
{
    UNHEALTHY(-1), READY(0), CACHE_UPDATING(0x01), INDEX_UPDATING(0x02);

    public final int StatusCode;
    WorkerStatus(int statusCode)
    {
        this.StatusCode = statusCode;
    }
}
