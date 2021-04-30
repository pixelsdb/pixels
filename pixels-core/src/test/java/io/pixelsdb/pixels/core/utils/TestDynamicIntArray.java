/*
 * Copyright 2020 PixelsDB.
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

import org.junit.Test;

/**
 * Created at: 20-1-8
 * Author: hank
 */
public class TestDynamicIntArray
{
    @Test
    public void test ()
    {
        DynamicIntArray array = new DynamicIntArray(64);
        for (int i = 0; i < 1024; ++i)
        {
            array.add(i);
        }
        int[] ints = array.toArray();
        for (int i = 0; i < array.size(); ++i)
        {
            assert i == ints[i];
        }
    }
}
