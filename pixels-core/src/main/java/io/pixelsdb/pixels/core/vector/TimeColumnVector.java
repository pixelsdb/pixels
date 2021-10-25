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
package io.pixelsdb.pixels.core.vector;

import java.sql.Time;
import java.util.Arrays;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.roundSqlTime;

/**
 * TimeColumnVector derived from io.pixelsdb.pixels.core.vector.TimestampColumnVector.
 * <p>
 * This class represents a nullable time column vector capable of handing a wide range of
 * time values.
 * <p>
 * We store the value field of a Time class in primitive arrays.
 * <p>
 * We do this to avoid an array of Java Time objects which would have poor storage
 * and memory access characteristics.
 * <p>
 * Generally, the caller will fill in a scratch time object with values from a row, work
 * using the scratch time, and then perhaps update the column vector row with a result.
 *
 * 2021-04-25
 * @author hank
 */
public class TimeColumnVector extends ColumnVector
{
    public int[] times;
    // The values from Time.getTime().

    /*
     * Scratch objects.
     */
    private final Time scratchTime;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public TimeColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Don't use this except for testing purposes.
     *
     * @param len the number of rows
     */
    public TimeColumnVector(int len)
    {
        super(len);

        times = new int[len];
        memoryUsage += Integer.BYTES * len;

        scratchTime = new Time(0);
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

    /**
     * Return a row's value, which is the millis in a day.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @return
     */
    public long getTime(int elementNum)
    {
        return times[elementNum];
    }

    /**
     * Set a Time object from a row of the column.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param t the time
     * @param elementNum
     */
    public void timeUpdate(Time t, int elementNum)
    {
        t.setTime(times[elementNum]);
    }

    /**
     * Return the scratch Time object set from a row.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @return
     */
    public Time asScratchTime(int elementNum)
    {
        scratchTime.setTime(times[elementNum]);
        return scratchTime;
    }

    /**
     * Return the scratch time (contents undefined).
     *
     * @return
     */
    public Time getScratchTime()
    {
        return scratchTime;
    }

    /**
     * Return a long representation of a time.
     *
     * @param elementNum
     * @return
     */
    public long getTimeAsLong(int elementNum)
    {
        scratchTime.setTime(times[elementNum]);
        return getTimeAsLong(scratchTime);
    }

    /**
     * Return a long representation of a Time.
     *
     * @param t the time
     * @return
     */
    public static long getTimeAsLong(Time t)
    {
        return millisToSeconds(t.getTime());
    }

    // Copy of TimestampWritable.millisToSeconds

    /**
     * Rounds the number of milliseconds relative to the epoch down to the nearest whole number of
     * seconds. 500 would round to 0, -500 would round to -1.
     */
    private static long millisToSeconds(long millis)
    {
        if (millis >= 0)
        {
            return millis / 1000;
        }
        else
        {
            return (millis - 999) / 1000;
        }
    }

    /**
     * Compare row to Time.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @param t the time
     * @return -1, 0, 1 standard compareTo values.
     */
    public int compareTo(int elementNum, Time t)
    {
        return asScratchTime(elementNum).compareTo(t);
    }

    /**
     * Compare Time to row.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param t the time
     * @param elementNum
     * @return -1, 0, 1 standard compareTo values.
     */
    public int compareTo(Time t, int elementNum)
    {
        return t.compareTo(asScratchTime(elementNum));
    }

    /**
     * Compare a row to another TimeColumnVector's row.
     *
     * @param elementNum1
     * @param timeColVector2
     * @param elementNum2
     * @return
     */
    public int compareTo(int elementNum1, TimeColumnVector timeColVector2,
                         int elementNum2)
    {
        return asScratchTime(elementNum1).compareTo(
                timeColVector2.asScratchTime(elementNum2));
    }

    /**
     * Compare another TimeColumnVector's row to a row.
     *
     * @param timeColVector1
     * @param elementNum1
     * @param elementNum2
     * @return
     */
    public int compareTo(TimeColumnVector timeColVector1, int elementNum1,
                         int elementNum2)
    {
        return timeColVector1.asScratchTime(elementNum1).compareTo(
                asScratchTime(elementNum2));
    }

    @Override
    public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector)
    {
        TimeColumnVector timeColVector = (TimeColumnVector) inputVector;

        times[outElementNum] = timeColVector.times[inputElementNum];
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof TimeColumnVector)
        {
            TimeColumnVector srcVector = (TimeColumnVector) inputVector;
            this.times = srcVector.times;
            this.isNull = srcVector.isNull;
            this.noNulls = srcVector.noNulls;
            this.isRepeating = srcVector.isRepeating;
            this.writeIndex = srcVector.writeIndex;
        }
    }

    /**
     * Simplify vector by brute-force flattening noNulls and isRepeating
     * This can be used to reduce combinatorial explosion of code paths in VectorExpressions
     * with many arguments.
     * @param selectedInUse whether use the selected indexes in sel or not.
     * @param sel the selected indexes.
     * @param size the size of sel or the number of values to flatten.
     */
    public void flatten(boolean selectedInUse, int[] sel, int size)
    {
        flattenPush();
        if (isRepeating)
        {
            isRepeating = false;
            int repeatFastTime = times[0];
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
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void add(Time value)
    {
        set(writeIndex++, value);
    }

    @Override
    public void add(String value)
    {
        set(writeIndex++, Time.valueOf(value));
    }

    /**
     * Set a row from a time.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param t the time
     */
    public void set(int elementNum, Time t)
    {
        if (t == null)
        {
            this.noNulls = false;
            this.isNull[elementNum] = true;
        }
        else
        {
            this.times[elementNum] = roundSqlTime(t.getTime());
        }
    }

    /**
     * Set a row from a value, which is the millis in the day.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param millis
     */
    public void set(int elementNum, int millis)
    {
        this.times[elementNum] = millis;
    }

    /**
     * Set a row from the current value in the scratch time.
     *
     * @param elementNum
     */
    public void setFromScratchTime(int elementNum)
    {
        // scratchTime may be changed outside this class, so we also mod it by millis in a day.
        this.times[elementNum] = roundSqlTime(scratchTime.getTime());
    }

    /**
     * Set row to standard null value(s).
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     */
    public void setNullValue(int elementNum)
    {
        times[elementNum] = 0;
    }

    // Copy the current object contents into the output. Only copy selected entries,
    // as indicated by selectedInUse and the sel array.
    public void copySelected(
            boolean selectedInUse, int[] sel, int size, TimeColumnVector output)
    {
        // Output has nulls if and only if input has nulls.
        output.noNulls = noNulls;
        output.isRepeating = false;

        // Handle repeating case
        if (isRepeating)
        {
            output.times[0] = times[0];
            output.isNull[0] = isNull[0];
            output.isRepeating = true;
            return;
        }

        // Handle normal case

        // Copy data values over
        if (selectedInUse)
        {
            for (int j = 0; j < size; j++)
            {
                int i = sel[j];
                output.times[i] = times[i];
            }
        }
        else
        {
            System.arraycopy(times, 0, output.times, 0, size);
        }

        // Copy nulls over if needed
        if (!noNulls)
        {
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    int i = sel[j];
                    output.isNull[i] = isNull[i];
                }
            }
            else
            {
                System.arraycopy(isNull, 0, output.isNull, 0, size);
            }
        }
    }

    /**
     * Fill all the vector entries with a time.
     *
     * @param t the time
     */
    public void fill(Time t)
    {
        noNulls = true;
        isRepeating = true;
        times[0] = roundSqlTime(t.getTime());
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
            scratchTime.setTime(times[row]);
            buffer.append(scratchTime.toString());
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
        int[] oldTime = times;
        times = new int[size];
        memoryUsage += Integer.BYTES * size;
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
}
