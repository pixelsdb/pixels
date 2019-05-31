/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.presto.block;

import com.facebook.presto.spi.block.AbstractVariableWidthBlock;
import com.facebook.presto.spi.block.Block;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.function.BiConsumer;

import static cn.edu.ruc.iir.pixels.presto.block.BlockUtil.checkArrayRange;
import static cn.edu.ruc.iir.pixels.presto.block.BlockUtil.checkValidRegion;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * This class refers to com.facebook.presto.spi.block.VariableWidthBlock and AbstractVariableWidthBlock.
 *
 * Modifies:
 * 1. we use a byte[][] instead of Slice as the backing storage
 * and replaced the implementation of each methods;
 * 2. reimplemented getSingleValueBlock here.
 *
 * Created at: 19-5-31
 * Author: hank
 */
public class VarcharArrayBlock extends AbstractVariableWidthBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VarcharArrayBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    private final byte[][] values;
    private final int[] starts;
    private final int[] lengths;
    private final boolean[] valueIsNull;

    private final long retainedSizeInBytes;
    private final long sizeInBytes;

    public VarcharArrayBlock(int positionCount, byte[][] values, int[] starts, int[] lengths, boolean[] valueIsNull)
    {
        this(0, positionCount, values, starts, lengths, valueIsNull);
    }

    VarcharArrayBlock(int arrayOffset, int positionCount, byte[][] values, int[] starts, int[] lengths, boolean[] valueIsNull)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values == null || values.length - arrayOffset < (positionCount)) {
            throw new IllegalArgumentException("values is null or its length is less than positionCount");
        }
        this.values = values;

        if (starts == null || starts.length - arrayOffset < (positionCount)) {
            throw new IllegalArgumentException("starts is null or its length is less than positionCount");
        }
        this.starts = starts;

        if (lengths == null || lengths.length - arrayOffset < (positionCount)) {
            throw new IllegalArgumentException("lengths is null or its length is less than positionCount");
        }
        this.lengths = lengths;

        if (valueIsNull == null || valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull is null or its length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        long size = 0L, retainedSize = 0L;
        for (int i = 0; i < positionCount; ++i)
        {
            size += lengths[arrayOffset + i];
            retainedSize += valueIsNull[arrayOffset + i] ? 0L : values[arrayOffset + i].length;
        }
        sizeInBytes = size;
        retainedSizeInBytes = INSTANCE_SIZE + retainedSize + sizeOf(valueIsNull) + sizeOf(starts) + sizeOf(lengths);
    }

    /**
     * Gets the start offset of the value at the {@code position}.
     */
    @Override
    protected final int getPositionOffset(int position)
    {
        return starts[position + arrayOffset];
    }

    /**
     * Gets the length of the value at the {@code position}.
     * This method must be implemented if @{code getSlice} is implemented.
     */
    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(position);
        return lengths[position + arrayOffset];
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull[position + arrayOffset];
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
        checkValidRegion(getPositionCount(), position, length);
        long size = 0L;
        for (int i = 0; i < length; ++i)
        {
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
        consumer.accept(starts, sizeOf(starts));
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
        checkArrayRange(positions, offset, length);
        byte[][] newValues = new byte[length][];
        int[] newStarts = new int[length];
        int[] newLengths = new int[length];
        boolean[] newValueIsNull = new boolean[length];

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (valueIsNull[position + arrayOffset]) {
                newValueIsNull[i] = true;
            }
            else {
                // we only copy the valid part of each value.
                newLengths[i] = lengths[position + arrayOffset];
                newValues[i] = Arrays.copyOfRange(values[position + arrayOffset],
                        starts[position + arrayOffset], newLengths[i]);
                // newStarts is 0.
            }
        }
        return new VarcharArrayBlock(length, newValues, newStarts, newLengths, newValueIsNull);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        // do not specify the offset and length for wrappedBuffer,
        // a raw slice should contain the whole bytes of value at the position.
        return Slices.wrappedBuffer(values[position + arrayOffset]);
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
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new VarcharArrayBlock(positionOffset + arrayOffset, length, values, starts, lengths, valueIsNull);
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
        byte[][] copy = new byte[1][];
        if (isNull(position)) {
            return new VarcharArrayBlock(1, copy, new int[] {0}, new int[] {0}, new boolean[] {true});
        }

        int offset = getPositionOffset(position);
        int entrySize = getSliceLength(position);
        copy[0] = Arrays.copyOfRange(values[position + arrayOffset],
                offset, entrySize);

        return new VarcharArrayBlock(1, copy, new int[] {0}, new int[] {entrySize}, new boolean[] {false});
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
        checkValidRegion(getPositionCount(), positionOffset, length);
        positionOffset += arrayOffset;

        byte[][] newValues = new byte[length][];
        int[] newStarts = new int[length];
        int[] newLengths = new int[length];
        boolean[] newValueIsNull = new boolean[length];

        for (int i = 0; i < length; i++) {
            if (valueIsNull[positionOffset + i]) {
                newValueIsNull[i] = true;
            }
            else {
                // we only copy the valid part of each value.
                newLengths[i] = lengths[positionOffset + i];
                newValues[i] = Arrays.copyOfRange(values[positionOffset + i],
                        starts[positionOffset + i], newLengths[i]);
                // newStarts is 0.
            }
        }
        return new VarcharArrayBlock(length, newValues, newStarts, newLengths, newValueIsNull);
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
