/*
 * Copyright 2017-2019 PixelsDB.
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

import com.google.flatbuffers.FlatBufferBuilder;
import io.pixelsdb.pixels.core.flat.ColumnVectorFlat;
import io.pixelsdb.pixels.core.flat.TimestampColumnVectorFlat;
import io.pixelsdb.pixels.core.utils.Bitmap;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.utils.DatetimeUtils.*;
import static java.util.Objects.requireNonNull;

/**
 * TimestampColumnVector derived from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * This class represents a nullable timestamp column vector capable of handing a wide range of
 * timestamp values.
 * <p>
 * We store the microseconds since the epoch of a Timestamp class in primitive arrays.
 * <p>
 * We do this to avoid an array of Java Timestamp objects which would have poor storage
 * and memory access performance.
 * <p>
 * Generally, the caller will fill in a scratch timestamp object with values from a row, work
 * using the scratch timestamp, and then perhaps update the column vector row with a result.
 */
public class TimestampColumnVector extends ColumnVector
{
    private int precision;

    /*
     * Microseconds from the epoch (1970-0-1).
     */
    public long[] times;

    /*
     * Scratch objects.
     */
    private final Timestamp scratchTimestamp;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public TimestampColumnVector(int precision)
    {
        this(VectorizedRowBatch.DEFAULT_SIZE, precision);
    }

    /**
     * Don't use this except for testing purposes.
     *
     * @param len the number of rows
     */
    public TimestampColumnVector(int len, int precision)
    {
        super(len);
        this.precision = precision;
        this.times = new long[len];
        this.memoryUsage += (long) Long.BYTES * len + Integer.BYTES;
        this.scratchTimestamp = new Timestamp(0);
    }

    public int getPrecision()
    {
        return precision;
    }

    /**
     * Return the number of rows.
     *
     * @return
     */
    public int getLength()
    {
        return times.length;
    }

    @Override
    public int[] accumulateHashCode(int[] hashCode)
    {
        requireNonNull(hashCode, "hashCode is null");
        checkArgument(hashCode.length > 0 && hashCode.length <= this.length, "",
                "the length of hashCode is not in the range [1, length]");
        for (int i = 0; i < hashCode.length; ++i)
        {
            if (this.isNull[i])
            {
                continue;
            }
            hashCode[i] = 31 * hashCode[i] + (int)(this.times[i] ^ (this.times[i] >>> 16));
        }
        return hashCode;
    }

    @Override
    public boolean elementEquals(int index, int otherIndex, ColumnVector other)
    {
        TimestampColumnVector otherVector = (TimestampColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            // if times are equal, precision does not matter.
            return this.times[index] == otherVector.times[otherIndex];
        }
        return false;
    }

    @Override
    public int compareElement(int index, int otherIndex, ColumnVector other)
    {
        TimestampColumnVector otherVector = (TimestampColumnVector) other;
        if (!this.isNull[index] && !otherVector.isNull[otherIndex])
        {
            // if times are equal, precision does not matter.
            return Long.compare(this.times[index], otherVector.times[otherIndex]);
        }
        return this.isNull[index] ? -1 : 1;
    }

    /**
     * Return the microseconds since the epoch.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @return
     */
    public long getMicros(int elementNum)
    {
        return times[elementNum];
    }

    /**
     * Set a Timestamp object from a row of the column.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param timestamp
     * @param elementNum
     */
    public void timestampUpdate(Timestamp timestamp, int elementNum)
    {
        timestamp.setTime(microsToMillis(times[elementNum]));
        timestamp.setNanos(microsToFracNanos(times[elementNum]));
    }

    /**
     * Return the scratch Timestamp object set from a row.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @return
     */
    public Timestamp asScratchTimestamp(int elementNum)
    {
        scratchTimestamp.setTime(microsToMillis(times[elementNum]));
        scratchTimestamp.setNanos(microsToFracNanos(times[elementNum]));
        return scratchTimestamp;
    }

    /**
     * Compare row to Timestamp.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @param timestamp
     * @return -1, 0, 1 standard compareTo values.
     */
    public int compareTo(int elementNum, Timestamp timestamp)
    {
        return asScratchTimestamp(elementNum).compareTo(timestamp);
    }

    /**
     * Compare Timestamp to row.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param timestamp
     * @param elementNum
     * @return -1, 0, 1 standard compareTo values.
     */
    public int compareTo(Timestamp timestamp, int elementNum)
    {
        return timestamp.compareTo(asScratchTimestamp(elementNum));
    }

    /**
     * Compare a row to another TimestampColumnVector's row.
     *
     * @param elementNum1
     * @param timestampColVector2
     * @param elementNum2
     * @return
     */
    public int compareTo(int elementNum1, TimestampColumnVector timestampColVector2,
                         int elementNum2)
    {
        return Long.compare(this.times[elementNum1], timestampColVector2.times[elementNum2]);
    }

    /**
     * Compare another TimestampColumnVector's row to a row.
     *
     * @param timestampColVector1
     * @param elementNum1
     * @param elementNum2
     * @return
     */
    public int compareTo(TimestampColumnVector timestampColVector1, int elementNum1,
                         int elementNum2)
    {
        return Long.compare(timestampColVector1.times[elementNum1], this.times[elementNum2]);
    }

    /**
     * <b>Note: </b> the user of this method should ensure that the input vector
     * does not have a higher precision than this vector.
     * @param inputIndex the index of the element in the input vector
     * @param inputVector the input vector
     */
    @Override
    public void addElement(int inputIndex, ColumnVector inputVector)
    {
        int index = writeIndex++;
        if (inputVector.noNulls || !inputVector.isNull[inputIndex])
        {
            isNull[index] = false;
            TimestampColumnVector in = (TimestampColumnVector) inputVector;
            /*
             * For performance considerations, we do not round the time.
             * The user of this method is responsible for checking the precision.
             */
            times[index] = in.times[inputIndex];
        }
        else
        {
            isNull[index] = true;
            noNulls = false;
        }
    }

    /**
     * <b>Note: </b> the user of this method should ensure that the source vector
     * does not have a higher precision than this vector.
     * @param selected the index of selected element in the source vector
     * @param offset the start offset in selected
     * @param length the length to process from the offset
     * @param src the source vector
     */
    @Override
    public void addSelected(int[] selected, int offset, int length, ColumnVector src)
    {
        // isRepeating should be false and src should be an instance of TimestampColumnVector.
        // However, we do not check these for performance considerations.
        TimestampColumnVector source = (TimestampColumnVector) src;

        for (int i = offset; i < offset + length; i++)
        {
            int srcIndex = selected[i], thisIndex = writeIndex++;
            if (source.isNull[srcIndex])
            {
                this.isNull[thisIndex] = true;
                this.noNulls = false;
            }
            else
            {
                /*
                 * For performance considerations, we do not round the time.
                 * The user of this method is responsible for checking the precision.
                 */
                this.times[thisIndex] = source.times[srcIndex];
                // this.nanos is currently not used.
                this.isNull[thisIndex] = false;
            }
        }
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof TimestampColumnVector)
        {
            TimestampColumnVector srcVector = (TimestampColumnVector) inputVector;
            this.times = srcVector.times;
            this.precision = srcVector.precision;
            this.isNull = srcVector.isNull;
            this.noNulls = srcVector.noNulls;
            this.isRepeating = srcVector.isRepeating;
            this.writeIndex = srcVector.writeIndex;
        }
    }

    @Override
    protected void applyFilter(Bitmap filter, int before)
    {
        checkArgument(!isRepeating,
                "column vector is repeating, flatten before applying filter");
        checkArgument(before > 0 && before <= length,
                "before index is not in the range [1, length]");
        boolean noNulls = true;
        int j = 0;
        for (int i = filter.nextSetBit(0);
             i >= 0 && i < before; i = filter.nextSetBit(i+1), j++)
        {
            if (i > j)
            {
                this.times[j] = this.times[i];
                // this.nanos is currently not used.
                this.isNull[j] = this.isNull[i];
            }
            if (this.isNull[j])
            {
                noNulls = false;
            }
            /*
             * The number of rows in a row batch is impossible to reach Integer.MAX_VALUE.
             * Therefore, we do not check overflow here.
             */
        }
        this.noNulls = noNulls;
    }

    // Simplify vector by brute-force flattening noNulls and isRepeating
    // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
    // with many arguments.
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        if (isRepeating)
        {
            isRepeating = false;
            long repeatFastTime = times[0];
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    int i = sel[j];
                    times[i] = repeatFastTime;
                }
            }
            else
            {
                Arrays.fill(times, 0, size, repeatFastTime);
            }
            writeIndex = size;
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void add(long micros)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        set(writeIndex++, micros);
    }

    @Override
    public void add(byte[] value)
    {
        if(checkBytesNull(value))
        {
            return;
        }

        if (value.length != Long.BYTES)
        {
            throw new IllegalArgumentException("Only byte[8] supported for serialization to long for timestamp");
        }
        long v = ByteBuffer.wrap(value).getLong();
        add(v);
    }


    @Override
    public void add(Timestamp value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        set(writeIndex++, value);
    }

    @Override
    public void add(String value)
    {
        if (writeIndex >= getLength())
        {
            ensureSize(writeIndex * 2, true);
        }
        set(writeIndex++, stringTimestampToMicros(value));
    }

    /**
     * Set a row from a timestamp.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param timestamp
     */
    public void set(int elementNum, Timestamp timestamp)
    {
        if (elementNum >= writeIndex)
        {
            writeIndex = elementNum + 1;
        }
        if (timestamp == null)
        {
            this.noNulls = false;
            this.isNull[elementNum] = true;
        }
        else
        {
            this.isNull[elementNum] = false;
            this.times[elementNum] = roundMicrosToPrecision(timestampToMicros(timestamp), precision);
        }
    }

    /**
     * Set a row from a time in long.
     * We assume the entry has already been isRepeated adjusted.
     *
     * <b>Note: </b> the user of this method should ensure the time
     * is rounded to the precision of this vector.
     *
     * @param elementNum the index of the element
     * @param time microseconds since the epoch
     */
    public void set(int elementNum, long time)
    {
        if (elementNum >= writeIndex)
        {
            writeIndex = elementNum + 1;
        }
        this.isNull[elementNum] = false;
        /*
         * For performance considerations, we do not round the time.
         * Because this method is used by Pixels reader, and the reader is responsible
         * for checking the precision.
         */
        this.times[elementNum] = time;
    }

    /**
     * Set row to standard null value(s).
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     */
    public void setNullValue(int elementNum)
    {
        if (elementNum >= writeIndex)
        {
            writeIndex = elementNum + 1;
        }
        isNull[elementNum] = true;
        times[elementNum] = 0;
        noNulls = false;
    }

    /**
     * Fill all the vector entries with a timestamp.
     *
     * @param timestamp
     */
    public void fill(Timestamp timestamp)
    {
        noNulls = true;
        isRepeating = true;
        times[0] = roundMicrosToPrecision(timestampToMicros(timestamp), precision);
    }

    @Override
    public void stringifyValue(StringBuilder buffer, int row)
    {
        if (isRepeating)
        {
            row = 0;
        }
        if (noNulls || !isNull[row])
        {
            scratchTimestamp.setTime(microsToMillis(times[row]));
            scratchTimestamp.setNanos(microsToFracNanos(times[row]));
            buffer.append(scratchTimestamp.toString());
        }
        else
        {
            buffer.append("null");
        }
    }

    @Override
    public void ensureSize(int size, boolean preserveData)
    {
        super.ensureSize(size, preserveData);
        if (size <= times.length)
        {
            return;
        }
        long[] oldTime = times;
        times = new long[size];
        memoryUsage += (long) Long.BYTES * size;
        length = size;
        if (preserveData)
        {
            if (isRepeating)
            {
                times[0] = oldTime[0];
            }
            else
            {
                System.arraycopy(oldTime, 0, times, 0, oldTime.length);
            }
        }
    }

    @Override
    public void close()
    {
        super.close();
        this.times = null;
    }

    @Override
    public byte getFlatBufferType()
    {
        return ColumnVectorFlat.TimestampColumnVectorFlat;
    }

    @Override
    public int serialize(FlatBufferBuilder builder)
    {
        int baseOffset = super.serialize(builder);
        int timesVectorOffset = TimestampColumnVectorFlat.createTimesVector(builder, times);
        TimestampColumnVectorFlat.startTimestampColumnVectorFlat(builder);
        TimestampColumnVectorFlat.addBase(builder, baseOffset);
        TimestampColumnVectorFlat.addPrecision(builder, precision);
        TimestampColumnVectorFlat.addTimes(builder, timesVectorOffset);
        TimestampColumnVectorFlat.addScratchTimestamp(builder, scratchTimestamp.getTime());
        return TimestampColumnVectorFlat.endTimestampColumnVectorFlat(builder);
    }

    public static TimestampColumnVector deserialize(TimestampColumnVectorFlat flat)
    {
        TimestampColumnVector vector = new TimestampColumnVector(flat.base().length(), flat.precision());
        for (int i = 0;i < flat.timesLength(); ++i)
        {
            vector.times[i] = flat.times(i);
        }
        vector.scratchTimestamp.setTime(flat.scratchTimestamp());
        vector.deserializeBase(flat.base());
        return vector;
    }
}
