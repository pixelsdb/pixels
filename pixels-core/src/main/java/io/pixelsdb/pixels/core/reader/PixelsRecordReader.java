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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;

/**
 * @author guodong
 */
public interface PixelsRecordReader
        extends AutoCloseable
{
    /**
     * Prepare for the next row batch.
     *
     * @param batchSize the willing batch size
     * @return the real batch size
     */
    int prepareBatch(int batchSize) throws IOException;

    /**
     * Read the next row batch.
     *
     * @param batchSize the row batch size
     * @return vectorized row batch
     * @throws java.io.IOException
     */
    VectorizedRowBatch readBatch(int batchSize)
            throws IOException;

    /**
     * Read the next row batch
     *
     * @return row batch
     * @throws java.io.IOException
     */
    VectorizedRowBatch readBatch()
            throws IOException;

    /**
     * Get current row number
     *
     * @return number of the row currently being read
     */
    long getRowNumber();

    /**
     * Seek to specified row
     *
     * @param rowIndex row index
     * @return seek success
     * @throws java.io.IOException
     */
    @Deprecated
    boolean seekToRow(long rowIndex)
            throws IOException;

    /**
     * Skip specified number of rows
     *
     * @param rowNum number of rows to skip
     * @return skip success
     * @throws java.io.IOException
     */
    @Deprecated
    boolean skip(long rowNum)
            throws IOException;

    long getCompletedBytes();

    long getReadTimeNanos();

    /**
     * Cleanup and release resources
     *
     * @throws java.io.IOException
     */
    @Override
    void close()
            throws IOException;
}
