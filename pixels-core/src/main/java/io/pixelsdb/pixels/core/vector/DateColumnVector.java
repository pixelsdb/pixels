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

import java.sql.Date;
import java.util.Arrays;

import static io.pixelsdb.pixels.core.utils.DatetimeUtils.*;

/**
 * DateColumnVector derived from io.pixelsdb.pixels.core.vector.TimestampColumnVector.
 * <p>
 * This class represents a nullable date column vector capable of handing a wide range of
 * date values.
 * <p>
 * We store the value field of a Date class in primitive arrays.
 * <p>
 * We do this to avoid an array of Java Date objects which would have poor storage
 * and memory access characteristics.
 * <p>
 * Generally, the caller will fill in a scratch date object with values from a row, work
 * using the scratch date, and then perhaps update the column vector row with a result.
 *
 * 2021-04-24
 * @author hank
 */
public class DateColumnVector extends ColumnVector
{
    /*
     * The storage arrays for this column vector corresponds to the storage of a Date:
     * They are the days from 1970-1-1. This is consistent with date type's internal
     * representation in Presto.
     */
    public int[] time;

    /*
     * Scratch objects.
     */
    private final Date scratchDate;

    /**
     * Use this constructor by default. All column vectors
     * should normally be the default size.
     */
    public DateColumnVector()
    {
        this(VectorizedRowBatch.DEFAULT_SIZE);
    }

    /**
     * Don't use this except for testing purposes.
     *
     * @param len the number of rows
     */
    public DateColumnVector(int len)
    {
        super(len);

        time = new int[len];

        scratchDate = new Date(0);
    }

    /**
     * Return the number of rows.
     *
     * @return
     */
    public int getLength()
    {
        return time.length;
    }

    /**
     * Return a row's value, which is the days from epoch (1970-1-1 0:0:0 UTC).
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @return the days from 1970-1-1.
     */
    public int getTime(int elementNum)
    {
        return time[elementNum];
    }

    /**
     * Set a Date object from a row of the column.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param date
     * @param elementNum
     */
    public void dateUpdate(Date date, int elementNum)
    {

        date.setTime(dayToMillis(time[elementNum]));
    }

    /**
     * Return the scratch Date object set from a row.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @return
     */
    public Date asScratchDate(int elementNum)
    {
        scratchDate.setTime(dayToMillis(time[elementNum]));
        return scratchDate;
    }

    /**
     * Return the scratch date (contents undefined).
     *
     * @return
     */
    public Date getScratchDate()
    {
        return scratchDate;
    }

    /**
     * Return a long representation of a date.
     *
     * @param elementNum
     * @return
     */
    public long getDateAsLong(int elementNum)
    {
        scratchDate.setTime(dayToMillis(time[elementNum]));
        return getDateAsLong(scratchDate);
    }

    /**
     * Return a long representation of a Date.
     *
     * @param date
     * @return
     */
    public static long getDateAsLong(Date date)
    {
        return millisToSeconds(date.getTime());
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
     * Compare row to Date.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param elementNum
     * @param date
     * @return -1, 0, 1 standard compareTo values.
     */
    public int compareTo(int elementNum, Date date)
    {
        return asScratchDate(elementNum).compareTo(date);
    }

    /**
     * Compare Date to row.
     * We assume the entry has already been NULL checked and isRepeated adjusted.
     *
     * @param date
     * @param elementNum
     * @return -1, 0, 1 standard compareTo values.
     */
    public int compareTo(Date date, int elementNum)
    {
        return date.compareTo(asScratchDate(elementNum));
    }

    /**
     * Compare a row to another DateColumnVector's row.
     *
     * @param elementNum1
     * @param dateColVector2
     * @param elementNum2
     * @return
     */
    public int compareTo(int elementNum1, DateColumnVector dateColVector2,
                         int elementNum2)
    {
        return asScratchDate(elementNum1).compareTo(
                dateColVector2.asScratchDate(elementNum2));
    }

    /**
     * Compare another DateColumnVector's row to a row.
     *
     * @param dateColVector1
     * @param elementNum1
     * @param elementNum2
     * @return
     */
    public int compareTo(DateColumnVector dateColVector1, int elementNum1,
                         int elementNum2)
    {
        return dateColVector1.asScratchDate(elementNum1).compareTo(
                asScratchDate(elementNum2));
    }

    @Override
    public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector)
    {
        DateColumnVector dateColVector = (DateColumnVector) inputVector;

        time[outElementNum] = dateColVector.time[inputElementNum];
    }

    @Override
    public void duplicate(ColumnVector inputVector)
    {
        if (inputVector instanceof DateColumnVector)
        {
            DateColumnVector srcVector = (DateColumnVector) inputVector;
            this.time = srcVector.time;
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
            int repeatFastTime = time[0];
            if (selectedInUse)
            {
                for (int j = 0; j < size; j++)
                {
                    int i = sel[j];
                    time[i] = repeatFastTime;
                }
            }
            else
            {
                Arrays.fill(time, 0, size, repeatFastTime);
            }
            flattenRepeatingNulls(selectedInUse, sel, size);
        }
        flattenNoNulls(selectedInUse, sel, size);
    }

    @Override
    public void add(Date value)
    {
        set(writeIndex++, value);
    }

    /**
     * Set a row from a date.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param date
     */
    public void set(int elementNum, Date date)
    {
        if (date == null)
        {
            this.noNulls = false;
            this.isNull[elementNum] = true;
        }
        else
        {
            this.time[elementNum] = millisToDay(date.getTime());
        }
    }

    /**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
     */
    public void set(int elementNum, int days)
    {
        this.time[elementNum] = days;
    }

    /**
     * Set a row from the current value in the scratch date.
     *
     * @param elementNum
     */
    public void setFromScratchDate(int elementNum)
    {
        this.time[elementNum] = millisToDay(scratchDate.getTime());
    }

    /**
     * Set row to standard null value(s).
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     */
    public void setNullValue(int elementNum)
    {
        time[elementNum] = 0;
    }

    // Copy the current object contents into the output. Only copy selected entries,
    // as indicated by selectedInUse and the sel array.
    public void copySelected(
            boolean selectedInUse, int[] sel, int size, DateColumnVector output)
    {
        // Output has nulls if and only if input has nulls.
        output.noNulls = noNulls;
        output.isRepeating = false;

        // Handle repeating case
        if (isRepeating)
        {
            output.time[0] = time[0];
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
                output.time[i] = time[i];
            }
        }
        else
        {
            System.arraycopy(time, 0, output.time, 0, size);
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
     * Fill all the vector entries with a date.
     *
     * @param date
     */
    public void fill(Date date)
    {
        noNulls = true;
        isRepeating = true;
        time[0] = millisToDay(date.getTime());
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
            scratchDate.setTime(dayToMillis(time[row]));
            buffer.append(scratchDate.toString());
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
        if (size <= time.length)
        {
            return;
        }
        int[] oldTime = time;
        time = new int[size];
        length = size;
        if (preserveData)
        {
            if (isRepeating)
            {
                time[0] = oldTime[0];
            }
            else
            {
                System.arraycopy(oldTime, 0, time, 0, oldTime.length);
            }
        }
    }

    @Override
    public void close()
    {
        super.close();
        this.time = null;
    }
}
