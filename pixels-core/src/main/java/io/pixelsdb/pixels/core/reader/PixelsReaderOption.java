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
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.predicate.PixelsPredicate;

import java.util.Optional;

/**
 * @author guodong, hank
 */
public class PixelsReaderOption
{
    private String[] includedCols = new String[0];
    private PixelsPredicate predicate = null;
    private boolean skipCorruptRecords = false;
    private boolean tolerantSchemaEvolution = true;    // this may lead to column missing due to schema evolution
    private boolean enableEncodedColumnVector = false; // whether read encoded column vectors directly when possible
    private boolean readIntColumnAsIntVector = false; // whether read int32 columns as int32 column vectors
    private long transId = -1L;
    private long transTimestamp = -1L; // -1 means no need to consider the timestamp when reading data
    private int rgStart = 0;
    private int rgLen = -1;     // -1 means reading to the end of the file

    public PixelsReaderOption() { }

    public PixelsReaderOption includeCols(String[] columnNames)
    {
        this.includedCols = columnNames;
        return this;
    }

    public String[] getIncludedCols()
    {
        return includedCols;
    }

    public PixelsReaderOption predicate(PixelsPredicate predicate)
    {
        this.predicate = predicate;
        return this;
    }

    public Optional<PixelsPredicate> getPredicate()
    {
        if (predicate == null)
        {
            return Optional.empty();
        }
        return Optional.of(predicate);
    }

    public PixelsReaderOption skipCorruptRecords(boolean skipCorruptRecords)
    {
        this.skipCorruptRecords = skipCorruptRecords;
        return this;
    }

    public boolean isSkipCorruptRecords()
    {
        return skipCorruptRecords;
    }

    public PixelsReaderOption transId(long transId)
    {
        this.transId = transId;
        return this;
    }

    public long getTransId()
    {
        return this.transId;
    }

    public PixelsReaderOption transTimestamp(long transTimestamp)
    {
        this.transTimestamp = transTimestamp;
        return this;
    }

    public long getTransTimestamp()
    {
        return this.transTimestamp;
    }

    public boolean hasValidTransTimestamp()
    {
        return this.transTimestamp >= 0L;
    }

    public PixelsReaderOption rgRange(int rgStart, int rgLen)
    {
        this.rgStart = rgStart;
        this.rgLen = rgLen;
        return this;
    }

    public int getRGStart()
    {
        return this.rgStart;
    }

    public int getRGLen()
    {
        return this.rgLen;
    }

    public PixelsReaderOption tolerantSchemaEvolution(boolean tolerantSchemaEvolution)
    {
        this.tolerantSchemaEvolution = tolerantSchemaEvolution;
        return this;
    }

    public boolean isTolerantSchemaEvolution()
    {
        return tolerantSchemaEvolution;
    }

    public PixelsReaderOption enableEncodedColumnVector(boolean enableEncodedColumnVector)
    {
        this.enableEncodedColumnVector = enableEncodedColumnVector;
        return this;
    }

    public boolean isEnableEncodedColumnVector()
    {
        return enableEncodedColumnVector;
    }

    public void readIntColumnAsIntVector(boolean readIntColumnAsIntVector)
    {
        this.readIntColumnAsIntVector = readIntColumnAsIntVector;
    }

    public boolean isReadIntColumnAsIntVector()
    {
        return readIntColumnAsIntVector;
    }
}
