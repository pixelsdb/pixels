/*
 * Copyright 2021 PixelsDB.
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
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static io.pixelsdb.pixels.presto.block.EncoderUtil.decodeNullBits;
import static io.pixelsdb.pixels.presto.block.EncoderUtil.encodeNullsAsBits;

/**
 * This class is derived from com.facebook.presto.spi.block.IntArrayBlockEncoding.
 *
 * Created at: 26/04/2021
 * Author: hank
 */
public class TimeArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "TIME_ARRAY";

    private static final TimeArrayBlockEncoding instance = new TimeArrayBlockEncoding();

    public static TimeArrayBlockEncoding Instance()
    {
        return instance;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeInt(block.getInt(position, 0));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount);

        int[] values = new int[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (!valueIsNull[position]) {
                values[position] = sliceInput.readInt();
            }
        }

        return new TimeArrayBlock(positionCount, valueIsNull, values);
    }
}
