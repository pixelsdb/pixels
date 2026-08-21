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

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.PICOS_PER_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LongTimeColumnVector} read layout and native TIME deserialization.
 *
 * @author gengdy
 * @create 2026-08-17
 */
public class TestLongTimeColumnVector
{
    @Test
    public void testDeserializeTimeWithRequestedLayout()
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

        VectorizedRowBatch longTimeBatch = VectorizedRowBatch.deserialize(
                serialized, TypeDescription.VectorLayout.TIME_AS_LONG_TIME);
        assertTrue(longTimeBatch.cols[0] instanceof LongTimeColumnVector);
        LongTimeColumnVector longTimeVector = (LongTimeColumnVector) longTimeBatch.cols[0];
        assertEquals(timeVector.getLength(), longTimeVector.getLength());
        assertEquals(timeVector.getWriteIndex(), longTimeVector.getWriteIndex());
        assertEquals(timeVector.noNulls, longTimeVector.noNulls);
        assertEquals(timeVector.getPrecision(), longTimeVector.getPrecision());
        for (int i = 0; i < millis.length; ++i)
        {
            assertFalse(longTimeVector.isNull[i]);
            assertEquals((long) millis[i] * PICOS_PER_MILLIS, longTimeVector.vector[i]);
        }
        assertTrue(longTimeVector.isNull[millis.length]);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLongTimeColumnVectorCannotBeSerialized()
    {
        TypeDescription.createTime(3)
                .createRowBatch(1, TypeDescription.VectorLayout.TIME_AS_LONG_TIME)
                .serialize();
    }
}
