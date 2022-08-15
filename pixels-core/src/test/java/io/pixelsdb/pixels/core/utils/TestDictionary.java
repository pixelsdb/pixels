/*
 * Copyright 2022 PixelsDB.
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

import io.pixelsdb.pixels.common.utils.Constants;
import org.junit.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.TreeMap;
import java.util.UUID;

/**
 * @author hank
 * @date 8/13/22
 */
public class TestDictionary
{
    private static long getGCTime()
    {
        long gc = 0;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans())
        {
            gc += bean.getCollectionTime();
        }
        return gc;
    }

    @Test
    public void testRedBlackTree()
    {
        Dictionary dict = new StringRedBlackTree(Constants.INIT_DICT_SIZE);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; ++i)
        {
            dict.add(UUID.randomUUID().toString().getBytes(), 0, 36);
        }
        System.out.println(System.currentTimeMillis() - startTime);
        System.out.println(getGCTime());
    }

    @Test
    public void testTreeMap()
    {
        TreeMap<ByteBuffer, Integer> dict = new TreeMap<>();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; ++i)
        {
            dict.put(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()), i);
        }
        System.out.println(System.currentTimeMillis() - startTime);
        System.out.println(getGCTime());
    }

    @Test
    public void testHashTable()
    {
        Dictionary dict = new HashTableDictionary(Constants.INIT_DICT_SIZE);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; ++i)
        {
            dict.add(UUID.randomUUID().toString().getBytes());
        }
        System.out.println(System.currentTimeMillis() - startTime);
        System.out.println(getGCTime());
    }
}
