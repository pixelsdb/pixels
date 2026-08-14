/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.core.vector;

import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.Test;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.PICOSECONDS_PER_MILLISECOND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author hank
 * @create 2026-08-09
 */
public class TestLongColumnVector
{
    @Test
    public void testDeserializeTimeWithLongLayout()
    {
        int[] millis = {0, 1, 3_723_004, 86_399_999};
        VectorizedRowBatch source = TypeDescription.createTime(3).createRowBatch(millis.length + 1);
        TimeColumnVector timeVector = (TimeColumnVector) source.cols[0];
        for (int value : millis)
        {
            timeVector.add(value);
        }
        timeVector.addNull();
        source.size = millis.length + 1;

        byte[] serialized = source.serialize();
        VectorizedRowBatch defaultBatch = VectorizedRowBatch.deserialize(serialized);
        assertTrue(defaultBatch.cols[0] instanceof TimeColumnVector);
        assertEquals(millis[2], ((TimeColumnVector) defaultBatch.cols[0]).times[2]);

        VectorizedRowBatch longBatch = VectorizedRowBatch.deserialize(
                serialized, TypeDescription.VectorLayout.TIME_AS_PICO_LONG);
        assertTrue(longBatch.cols[0] instanceof LongColumnVector);
        LongColumnVector longVector = (LongColumnVector) longBatch.cols[0];
        assertEquals(timeVector.getLength(), longVector.getLength());
        assertEquals(timeVector.getWriteIndex(), longVector.getWriteIndex());
        assertFalse(longVector.noNulls);
        for (int i = 0; i < millis.length; ++i)
        {
            assertFalse(longVector.isNull[i]);
            assertEquals(millis[i] * PICOSECONDS_PER_MILLISECOND, longVector.vector[i]);
        }
        assertTrue(longVector.isNull[millis.length]);
    }
}
