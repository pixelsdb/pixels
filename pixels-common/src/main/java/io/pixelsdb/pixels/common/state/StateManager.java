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
package io.pixelsdb.pixels.common.state;

import io.etcd.jetcd.Watch;

/**
 * @author hank
 * @create 2024-04-19
 */
public class StateManager
{
    public StateManager()
    {

    }

    public void setState(String key, String value)
    {

    }

    public void clearState(String key)
    {

    }

    public Watch.Watcher onStateChange()
    {
        return null;
    }
}
