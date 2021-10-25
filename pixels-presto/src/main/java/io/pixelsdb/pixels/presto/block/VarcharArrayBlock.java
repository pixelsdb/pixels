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
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;
import org.openjdk.jol.info.ClassLayout;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * This class is derived from com.facebook.presto.spi.block.VariableWidthBlock and AbstractVariableWidthBlock.
 * <p>
 * Our main modifications:
 * 1. we use a byte[][] instead of Slice as the backing storage
 * and replaced the implementation of each methods;
 * 2. add some other methods.
 * <p>
 * Created at: 19-5-31
 * Author: hank
 */
public class VarcharArrayBlock implements Block
{
    static final Unsafe unsafe;
    static final long address;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VarcharArrayBlock.class).instanceSize();

    private final int arrayOffset; // start index of the valid items in offsets and length, usually 0.
    private final int positionCount; // number of items in this block.
    private final byte[][] values; // values of the items/
    private final int[] offsets; // start byte offset of the item in each value, \
    // always 0 if this block is deserialized by VarcharArrayBlockEncoding.readBlock.
    private final int[] lengths; // byte length of each item.
    private final boolean[] valueIsNull; // isNull flag of each item.

    private final long retainedSizeInBytes;
    private final long sizeInBytes;

    static
    {
        try
        {
            /**
             * refer to io.airlift.slice.JvmUtils
             */
            // fetch theUnsafe object
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null)
            {
                throw new RuntimeException("Unsafe access not available");
            }
            address = ARRAY_BYTE_BASE_OFFSET;
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public VarcharArrayBlock(int positionCount, byte[][] values, int[] offsets, int[] lengths, boolean[] valueIsNull)
    {
        this(0, positionCount, values, offsets, lengths, valueIsNull);
    }

    VarcharArrayBlock(int arrayOffset, int positionCount, byte[][] values, int[] offsets, int[] lengths, boolean[] valueIsNull)
    {
        if (arrayOffset < 0)
        {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0)
        {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values == null || values.length - arrayOffset < (positionCount))
        {
            throw new IllegalArgumentException("values is null or its length is less than positionCount");
        }
        this.values = values;

        if (offsets == null || offsets.length - arrayOffset < (positionCount))
        {
            throw new IllegalArgumentException("offsets is null or its length is less than positionCount");
        }
        this.offsets = offsets;

        if (lengths == null || lengths.length - arrayOffset < (positionCount))
        {
            throw new IllegalArgumentException("lengths is null or its length is less than positionCount");
        }
        this.lengths = lengths;

        if (valueIsNull == null || valueIsNull.length - arrayOffset < positionCount)
        {
            throw new IllegalArgumentException("valueIsNull is null or its length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        long size = 0L, retainedSize = 0L;
        Set<byte[]> existingValues = new HashSet<>(2);
        for (int i = 0; i < positionCount; ++i)
        {
            size += lengths[arrayOffset + i];
            // retainedSize should count the physical footprint of the values.
            if (!valueIsNull[arrayOffset + i])
            {
                if (!existingValues.contains(values[arrayOffset + i]))
                {
                    existingValues.add(values[arrayOffset + i]);
                    retainedSize += values[arrayOffset + i].length;
                }
            }
        }
        existingValues.clear();
        sizeInBytes = size;
        retainedSizeInBytes = INSTANCE_SIZE + retainedSize + sizeOf(values) +
                sizeOf(valueIsNull) + sizeOf(offsets) + sizeOf(lengths);
    }

    /**
     * Gets the start offset of the value at the {@code position}.
     */
    protected final int getPositionOffset(int position)
    {
        /**
         * Issue #132:
         * FIX: null must be checked here as offsets (i.e. starts) in column vector
         * may be reused in vectorized row batch and is not reset.
         */
        if (valueIsNull[position + arrayOffset])
        {
            return 0;
        }
        return offsets[position + arrayOffset];
    }

    /**
     * Gets the length of the value at the {@code position}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(position);
        /**
         * Issue #132:
         * FIX: null must be checked here as lengths (i.e. lens) in column vector
         * may be reused in vectorized row batch and is not reset.
         */
        if (valueIsNull[position + arrayOffset])
        {
            return 0;
        }
        return lengths[position + arrayOffset];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    /**
     * Returns the logical size of {@code block.getRegion(position, length)} in memory.
     * The method can be expensive. Do not use it outside an implementation of Block.
     */
    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        BlockUtil.checkValidRegion(getPositionCount(), position, length);
        long size = 0L;
        for (int i = 0; i < length; ++i)
        {
            // lengths[i] is zero if valueIsNull[i] is true, no need to check.
            size += lengths[position + arrayOffset + i];
        }
        return size + ((Integer.BYTES + Byte.BYTES) * (long) length);
    }

    /**
     * Returns the retained size of this block in memory.
     * This method is called from the inner most execution loop and must be fast.
     */
    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    /**
     * {@code consumer} visits each of the internal data container and accepts the size for it.
     * This method can be helpful in cases such as memory counting for internal data structure.
     * Also, the method should be non-recursive, only visit the elements at the top level,
     * and specifically should not call retainedBytesForEachPart on nested blocks
     * {@code consumer} should be called at least once with the current block and
     * must include the instance size of the current block
     */
    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        long retainedSize = 0L;
        for (int i = 0; i < positionCount; ++i)
        {
            retainedSize += valueIsNull[arrayOffset + i] ? 0L : values[arrayOffset + i].length;
        }
        consumer.accept(values, retainedSize);
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(lengths, sizeOf(lengths));
        consumer.accept(valueIsNull, sizeOf(valueIsNull));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    /**
     * Returns a block containing the specified positions.
     * Positions to copy are stored in a subarray within {@code positions} array
     * that starts at {@code offset} and has length of {@code length}.
     * All specified positions must be valid for this block.
     * <p>
     * The returned block must be a compact representation of the original block.
     */
    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        BlockUtil.checkArrayRange(positions, offset, length);
        byte[][] newValues = new byte[length][];
        int[] newStarts = new int[length];
        int[] newLengths = new int[length];
        boolean[] newValueIsNull = new boolean[length];

        for (int i = 0; i < length; i++)
        {
            int position = positions[offset + i];
            if (valueIsNull[position + arrayOffset])
            {
                newValueIsNull[i] = true;
            } else
            {
                // we only copy the valid part of each value.
                int from = offsets[position + arrayOffset];
                newLengths[i] = lengths[position + arrayOffset];
                newValues[i] = Arrays.copyOfRange(values[position + arrayOffset],
                        from, from + newLengths[i]);
                // newStarts is 0.
            }
        }
        return new VarcharArrayBlock(length, newValues, newStarts, newLengths, newValueIsNull);
    }

    protected Slice getRawSlice(int position)
    {
        // do not specify the offset and length for wrappedBuffer,
        // a raw slice should contain the whole bytes of value at the position.
        if (valueIsNull[position + arrayOffset])
        {
            if (values[position + arrayOffset] != null)
            {
                throw new IllegalArgumentException("value is not null.");
            }
            return Slices.EMPTY_SLICE;
        }
        if (values[position + arrayOffset] == null)
        {
            throw new IllegalArgumentException("value is null.");
        }
        return Slices.wrappedBuffer(values[position + arrayOffset]);
    }

    protected byte[] getRawValue(int position)
    {
        if (valueIsNull[position + arrayOffset])
        {
            if (values[position + arrayOffset] != null)
            {
                throw new IllegalArgumentException("value is not null.");
            }
            return null;
        }
        if (values[position + arrayOffset] == null)
        {
            throw new IllegalArgumentException("value is null.");
        }
        return values[position + arrayOffset];
    }

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     * <p>
     * The region can be a view over this block.  If this block is released
     * the region block may also be released.  If the region block is released
     * this block may also be released.
     */
    @Override
    public Block getRegion(int positionOffset, int length)
    {
        BlockUtil.checkValidRegion(getPositionCount(), positionOffset, length);

        return new VarcharArrayBlock(positionOffset + arrayOffset, length, values, offsets, lengths, valueIsNull);
    }

    /**
     * Gets the value at the specified position as a single element block.  The method
     * must copy the data into a new block.
     * <p>
     * This method is useful for operators that hold on to a single value without
     * holding on to the entire block.
     *
     * @throws IllegalArgumentException if this position is not valid
     */
    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        byte[][] copy = new byte[1][];
        if (isNull(position))
        {
            return new VarcharArrayBlock(1, copy, new int[]{0}, new int[]{0}, new boolean[]{true});
        }

        int offset = offsets[position + arrayOffset];
        int entrySize = lengths[position + arrayOffset];
        copy[0] = Arrays.copyOfRange(values[position + arrayOffset],
                offset, offset + entrySize);

        return new VarcharArrayBlock(1, copy, new int[]{0}, new int[]{entrySize}, new boolean[]{false});
    }

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     * <p>
     * The region returned must be a compact representation of the original block, unless their internal
     * representation will be exactly the same. This method is useful for
     * operators that hold on to a range of values without holding on to the
     * entire block.
     */
    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        BlockUtil.checkValidRegion(getPositionCount(), positionOffset, length);
        positionOffset += arrayOffset;

        byte[][] newValues = new byte[length][];
        int[] newStarts = new int[length];
        int[] newLengths = new int[length];
        boolean[] newValueIsNull = new boolean[length];

        for (int i = 0; i < length; i++)
        {
            if (valueIsNull[positionOffset + i])
            {
                newValueIsNull[i] = true;
            } else
            {
                // we only copy the valid part of each value.
                newLengths[i] = lengths[positionOffset + i];
                newValues[i] = Arrays.copyOfRange(values[positionOffset + i],
                        offsets[positionOffset + i], offsets[positionOffset + i] + newLengths[i]);
                // newStarts is 0.
            }
        }
        return new VarcharArrayBlock(length, newValues, newStarts, newLengths, newValueIsNull);
    }

    @Override
    public String getEncodingName()
    {
        return VarcharArrayBlockEncoding.NAME;
    }

    @Override
    public byte getByte(int position, int offset)
    {
        checkReadablePosition(position);
        return unsafe.getByte(getRawValue(position), address + getPositionOffset(position) + offset);
    }

    @Override
    public short getShort(int position, int offset)
    {
        checkReadablePosition(position);
        return unsafe.getShort(getRawValue(position), address + getPositionOffset(position) + offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        return unsafe.getInt(getRawValue(position), address + getPositionOffset(position) + offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        return unsafe.getLong(getRawValue(position), address + getPositionOffset(position) + offset);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return getRawSlice(position).slice(getPositionOffset(position) + offset, length);
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        checkReadablePosition(position);
        if (valueIsNull[position + arrayOffset])
        {
            return false;
        }
        Slice rawSlice = getRawSlice(position);
        if (getSliceLength(position) < length)
        {
            return false;
        }
        return otherBlock.bytesEqual(otherPosition, otherOffset, rawSlice, getPositionOffset(position) + offset, length);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        checkReadablePosition(position);
        if (valueIsNull[position + arrayOffset])
        {
            return false;
        }
        return getRawSlice(position).equals(getPositionOffset(position) + offset, length, otherSlice, otherOffset, length);
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        checkReadablePosition(position);
        return XxHash64.hash(getRawSlice(position), getPositionOffset(position) + offset, length);
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        if (valueIsNull[position + arrayOffset])
        {
            return -1;
        }
        Slice rawSlice = getRawSlice(position);
        if (getSliceLength(position) < length)
        {
            throw new IllegalArgumentException("Length longer than value length");
        }
        return -otherBlock.bytesCompare(otherPosition, otherOffset, otherLength, rawSlice, getPositionOffset(position) + offset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        checkReadablePosition(position);
        if (valueIsNull[position + arrayOffset])
        {
            return -1;
        }
        return getRawSlice(position).compareTo(getPositionOffset(position) + offset, length, otherSlice, otherOffset, otherLength);
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull[position + arrayOffset];
    }

    protected void checkReadablePosition(int position)
    {
        BlockUtil.checkValidPosition(position, getPositionCount());
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeBytes(getRawSlice(position), getPositionOffset(position) + offset, length);
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        writeBytesTo(position, 0, getSliceLength(position), blockBuilder);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VarcharArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", size=").append(sizeInBytes);
        sb.append(", retainedSize=").append(retainedSizeInBytes);
        sb.append('}');
        return sb.toString();
    }
}
