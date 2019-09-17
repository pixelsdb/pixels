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

import io.pixelsdb.pixels.core.PixelsPredicate;

import java.util.Optional;

/**
 * @author guodong
 */
public class PixelsReaderOption
{
    private String[] includedCols = new String[0];
    private PixelsPredicate predicate = null;
    private boolean skipCorruptRecords = false;
    private boolean tolerantSchemaEvolution = true;    // this may lead to column missing due to schema evolution
    private int rgStart = 0;
    private int rgLen = -1;     // -1 means reading to the end of the file

    public PixelsReaderOption()
    {
    }

    public void includeCols(String[] columnNames)
    {
        this.includedCols = columnNames;
    }

    public String[] getIncludedCols()
    {
        return includedCols;
    }

    public void predicate(PixelsPredicate predicate)
    {
        this.predicate = predicate;
    }

    public Optional<PixelsPredicate> getPredicate()
    {
        if (predicate == null)
        {
            return Optional.empty();
        }
        return Optional.of(predicate);
    }

    public void skipCorruptRecords(boolean skipCorruptRecords)
    {
        this.skipCorruptRecords = skipCorruptRecords;
    }

    public boolean isSkipCorruptRecords()
    {
        return skipCorruptRecords;
    }

    public void rgRange(int rgStart, int rgLen)
    {
        this.rgStart = rgStart;
        this.rgLen = rgLen;
    }

    public int getRGStart()
    {
        return this.rgStart;
    }

    public int getRGLen()
    {
        return this.rgLen;
    }

    public void tolerantSchemaEvolution(boolean tolerantSchemaEvolution)
    {
        this.tolerantSchemaEvolution = tolerantSchemaEvolution;
    }

    public boolean isTolerantSchemaEvolution()
    {
        return tolerantSchemaEvolution;
    }
}
