/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.cache;

import org.junit.Test;

/**
 * @author guodong
 */
public class TestDynamicArray
{
    private static final int TEST_NUM = 102508;

    @Test
    public void testAdd()
    {
        DynamicArray<Integer> array = new DynamicArray<>();
        for (int i = 0; i < TEST_NUM; i++)
        {
            array.add(i);
        }
        assert array.size() == TEST_NUM;
        System.out.println("Capacity: " + array.capacity());
        for (int i = 0; i < TEST_NUM; i++)
        {
            assert i == array.get(i);
        }
    }

    @Test
    public void testSet()
    {
        DynamicArray<Integer> array = new DynamicArray<>();
        for (int i = 0; i < TEST_NUM; i++)
        {
            array.add(i);
        }
        for (int i = 0; i < TEST_NUM; i++)
        {
            array.set(i, i * 2);
        }
        assert array.size() == TEST_NUM;
        for (int i = 0; i < TEST_NUM; i++)
        {
            assert i * 2 == array.get(i);
        }
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testException()
    {
        DynamicArray<Integer> array = new DynamicArray<>();
        array.get(Integer.MAX_VALUE);
    }
}
