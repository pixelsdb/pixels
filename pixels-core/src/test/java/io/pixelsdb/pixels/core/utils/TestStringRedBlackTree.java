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

import java.util.TreeMap;
import java.util.UUID;

/**
 * @author hank
 * @date 8/13/22
 */
public class TestStringRedBlackTree
{
    @Test
    public void test()
    {
        StringRedBlackTree dict = new StringRedBlackTree(Constants.INIT_DICT_SIZE);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000_000; ++i)
        {
            dict.add(UUID.randomUUID().toString().getBytes(), 0, 36);
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    @Test
    public void testTreeMap()
    {
        System.out.println(UUID.randomUUID().toString());
        TreeMap<String, Integer> dict = new TreeMap<>();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000_000; ++i)
        {
            dict.put(UUID.randomUUID().toString(), i);
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }
}
