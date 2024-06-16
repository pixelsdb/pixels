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
package io.pixelsdb.pixels.core.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class BlockingMap<K, V>
{
    private static final Logger logger = LogManager.getLogger(BlockingMap.class);
    private final Map<K, ArrayBlockingQueue<V>> map = new ConcurrentHashMap<>();

    private BlockingQueue<V> getQueue(K key)
    {
        assert (key != null);
        return map.computeIfAbsent(key, k -> new ArrayBlockingQueue<>(1));
    }

    public void put(K key, V value)
    {
        // This will throw an exception if the key is already present in the map - we've set the capacity of the queue to 1.
        // We can also use queue.offer(value) if we do not want an exception thrown.
        if (!getQueue(key).add(value))
        {
            logger.error("Ignoring duplicate key");
        }
    }

    public V get(K key) throws InterruptedException
    {
        V ret = getQueue(key).poll(60, TimeUnit.SECONDS);
        if (ret == null)
        {
            throw new RuntimeException("BlockingMap.get() timed out");
        }
        return ret;
    }

    public boolean exist(K key)
    {
        return getQueue(key).peek() != null;
    }

    public V get(K key, long timeout, TimeUnit unit) throws InterruptedException
    {
        return getQueue(key).poll(timeout, unit);
    }

    public boolean isEmpty()
    {
        for (BlockingQueue<V> queue : map.values())
        {
            if (!queue.isEmpty())
            {
                return false;
            }
        }
        return true;
    }
}
