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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hank
 * @create 2024-06-17
 */
public enum CoordinatorStatus
{
    DEAD(-1), INIT(0), READY(0x01);

    public final int StatusCode;

    CoordinatorStatus(int statusCode)
    {
        this.StatusCode = statusCode;
    }

    private static final AtomicInteger CurrentStatus = new AtomicInteger(CoordinatorStatus.INIT.StatusCode);

    public static void updateCurrentStatus(CoordinatorStatus status)
    {
        int currStatus = CurrentStatus.get();
        while (CurrentStatus.compareAndSet(currStatus, currStatus | status.StatusCode))
        {
            currStatus = CurrentStatus.get();
        }
    }

    public static int getCurrentStatus()
    {
        return CurrentStatus.get();
    }
}