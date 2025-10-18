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
package io.pixelsdb.pixels.common.index;

import org.junit.Test;

import java.util.Random;
import java.util.TreeSet;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author hank
 * @create 2025-10-18
 */
public class TestRowIdRange
{
    @Test
    public void test1()
    {
        TreeSet<RowIdRange> ranges = new TreeSet<>();
        for (long i = 0; i < 100 * 10000000; i += 100)
        {
            ranges.add(new RowIdRange(i, i + 100, 0, 0, 0, 100));
        }

        Random rand = new Random();
        long start = System.currentTimeMillis();
        for (long i = 0; i < 100000000; i += 100)
        {
            long rowId = rand.nextInt(100*10000000);
            RowIdRange range = new RowIdRange(rowId, Long.MAX_VALUE, 0, 0, 0, 1);
            RowIdRange found = ranges.floor(range);
            assertNotNull(found);
            assertTrue(found.getRowIdStart() <= rowId && rowId < found.getRowIdEnd());
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void test2()
    {
        TreeSet<Object> ranges = new TreeSet<>((o1, o2) -> {
            if (o1 instanceof RowIdRange)
            {
                return ((RowIdRange) o1).compareTo(((RowIdRange) o2));
            }
            else
            {
                long rowId = (Long) o1;
                RowIdRange range = (RowIdRange) o2;
                return range.getRowIdEnd() <= rowId ? 1 : range.getRowIdStart() > rowId ? -1 : 0;
            }
        });
        for (long i = 0; i < 100 * 10000000; i += 100)
        {
            ranges.add(new RowIdRange(i, i + 100, 0, 0, 0, 100));
        }

        Random rand = new Random();
        long start = System.currentTimeMillis();
        for (long i = 0; i < 100000000; i += 100)
        {
            long rowId = rand.nextInt(100*10000000);
            RowIdRange found = (RowIdRange) ranges.floor(rowId);
            assertNotNull(found);
            assertTrue(found.getRowIdStart() <= rowId && rowId < found.getRowIdEnd());
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}
