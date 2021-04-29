package io.pixelsdb.pixels.presto.block;

import com.facebook.presto.spi.block.*;
import com.facebook.presto.spi.type.TypeManager;
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
    public static final BlockEncodingFactory<TimeArrayBlockEncoding> FACTORY = new TimeArrayBlockEncoding.TimeArrayBlockEncodingFactory();
    private static final String NAME = "TIME_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
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
    public Block readBlock(SliceInput sliceInput)
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

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class TimeArrayBlockEncodingFactory
            implements BlockEncodingFactory<TimeArrayBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public TimeArrayBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return new TimeArrayBlockEncoding();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, TimeArrayBlockEncoding blockEncoding)
        {
        }
    }
}
