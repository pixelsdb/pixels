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

import io.pixelsdb.pixels.common.utils.EtcdUtil;

import static java.util.Objects.requireNonNull;

/**
 * The manager of a state stored in Etcd.
 * @author hank
 * @create 2024-04-19
 */
public class StateManager
{
    private final String key;

    /**
     * Create a state manager for the state with a key.
     * @param key the key
     */
    public StateManager(String key)
    {
        this.key = requireNonNull(key, "key is null");
    }

    /**
     * Update the state by setting a value for the state key.
     * @param value the value
     */
    public void setState(String value)
    {
        EtcdUtil.Instance().putKeyValue(key, value);
    }

    /**
     * Delete the state key-value pair.
     */
    public void deleteState()
    {
        EtcdUtil.Instance().delete(key);
    }

    /**
     * Delete all the states by the prefix of their keys.
     * @param keyPredix the predix of the state keys
     */
    public void deleteAllStatesByPrefix(String keyPredix)
    {
        EtcdUtil.Instance().deleteByPrefix(keyPredix);
    }
}
