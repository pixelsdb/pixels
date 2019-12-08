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
package io.pixelsdb.pixels.presto.block;

import com.facebook.presto.spi.block.Block;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

/**
 * This class is derived from com.facebook.presto.spi.block.EncoderUtil
 * Created at: 19-6-1
 * Author: hank
 */
public class EncoderUtil
{
    private EncoderUtil()
    {
    }

    /**
     * Append null values for the block as a stream of bits.
     */
    @SuppressWarnings({"NarrowingCompoundAssignment", "ImplicitNumericConversion"})
    public static void encodeNullsAsBits(SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < (positionCount & ~0b111); position += 8)
        {
            byte value = 0;
            value |= block.isNull(position) ? 0b1000_0000 : 0;
            value |= block.isNull(position + 1) ? 0b0100_0000 : 0;
            value |= block.isNull(position + 2) ? 0b0010_0000 : 0;
            value |= block.isNull(position + 3) ? 0b0001_0000 : 0;
            value |= block.isNull(position + 4) ? 0b0000_1000 : 0;
            value |= block.isNull(position + 5) ? 0b0000_0100 : 0;
            value |= block.isNull(position + 6) ? 0b0000_0010 : 0;
            value |= block.isNull(position + 7) ? 0b0000_0001 : 0;
            sliceOutput.appendByte(value);
        }

        // write last null bits
        if ((positionCount & 0b111) > 0)
        {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++)
            {
                value |= block.isNull(position) ? mask : 0;
                mask >>>= 1;
            }
            sliceOutput.appendByte(value);
        }
    }

    /**
     * Decode the bit stream created by encodeNullsAsBits.
     */
    public static boolean[] decodeNullBits(SliceInput sliceInput, int positionCount)
    {
        // read null bits 8 at a time
        boolean[] valueIsNull = new boolean[positionCount];
        for (int position = 0; position < (positionCount & ~0b111); position += 8)
        {
            byte value = sliceInput.readByte();
            valueIsNull[position] = ((value & 0b1000_0000) != 0);
            valueIsNull[position + 1] = ((value & 0b0100_0000) != 0);
            valueIsNull[position + 2] = ((value & 0b0010_0000) != 0);
            valueIsNull[position + 3] = ((value & 0b0001_0000) != 0);
            valueIsNull[position + 4] = ((value & 0b0000_1000) != 0);
            valueIsNull[position + 5] = ((value & 0b0000_0100) != 0);
            valueIsNull[position + 6] = ((value & 0b0000_0010) != 0);
            valueIsNull[position + 7] = ((value & 0b0000_0001) != 0);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0)
        {
            byte value = sliceInput.readByte();
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++)
            {
                valueIsNull[position] = ((value & mask) != 0);
                mask >>>= 1;
            }
        }

        return valueIsNull;
    }
}
