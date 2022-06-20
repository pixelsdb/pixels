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

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;

/**
 * @author guodong
 */
public interface PixelsRecordReader
        extends AutoCloseable
{
    /**
     * Prepare for the next row batch. This method is independent from readBatch().
     *
     * @param batchSize the willing batch size
     * @return the real batch size
     */
    int prepareBatch(int batchSize) throws IOException;

    /**
     * Read the next row batch. This method is thread-safe and independent from prepareBatch().
     *
     * @param batchSize the row batch size
     * @return vectorized row batch
     * @throws java.io.IOException
     */
    VectorizedRowBatch readBatch(int batchSize, boolean reuse)
            throws IOException;

    /**
     * Read the next row batch. This method is thread-safe and independent from prepareBatch().
     * The returned row batch is not reused across multiple calls. It is equivalent
     * to readBatch(batchSize, false).
     *
     * @param batchSize the row batch size
     * @return vectorized row batch
     * @throws java.io.IOException
     */
    VectorizedRowBatch readBatch(int batchSize)
            throws IOException;

    /**
     * Read the next row batch. This method is thread-safe and independent from prepareBatch().
     * It is equivalent to readBatch(DEFAULT_SIZE, reuse).
     *
     * @return row batch
     * @throws java.io.IOException
     */
    VectorizedRowBatch readBatch(boolean reuse)
            throws IOException;

    /**
     * Read the next row batch. This method is thread-safe and independent from prepareBatch().
     * The returned row batch is not reused across multiple calls. It is equivalent
     * to readBatch(DEFAULT_SIZE, false).
     *
     * @return row batch
     * @throws java.io.IOException
     */
    VectorizedRowBatch readBatch()
            throws IOException;

    /**
     * Get the schema of the included columns in the read option.
     *
     * @return result schema, null if PixelsRecordReader is not initialized successfully.
     */
    TypeDescription getResultSchema();

    /**
     * This method is valid after the construction of the PixelsRecordReader. It can be used
     * to check whether the record reader is currently readable. If any read-option is illegal,
     * the file is corrupted, or any fatal error occurred during the read, this method should
     * return false. In this case, the behavior of all the other methods in this record reader
     * is undetermined.
     * <p/>
     * However, if there is no more data to be read (i.e., EOF), the return value is undetermined.
     * In this case, {@code isEndOfFile()} should be used to check the EOF.
     *
     * @return false if this record reader is invalid
     */
    boolean isValid ();

    /**
     * This method is valid after calling prepareBatch or readBatch.
     * Before that, it will always return false.
     *
     * @return true if reach EOF
     */
    boolean isEndOfFile ();

    /**
     * Get current row number
     *
     * @return the number of the rows have been read
     */
    long getRowNumber();

    /**
     * Seek to specified row
     *
     * @param rowIndex row index
     * @return seek success
     * @throws java.io.IOException
     */
    boolean seekToRow(long rowIndex)
            throws IOException;

    /**
     * Skip specified number of rows
     *
     * @param rowNum number of rows to skip
     * @return skip success
     * @throws java.io.IOException
     */
    boolean skip(long rowNum)
            throws IOException;

    long getCompletedBytes();

    long getReadTimeNanos();

    /**
     * Get the approximate (may be slightly lower than actual)
     * <b>cumulative</b> memory usage, which is more meaningful for GC
     * performance tuning.
     * @return
     */
    long getMemoryUsage();

    /**
     * Cleanup and release resources
     *
     * @throws java.io.IOException
     */
    @Override
    void close()
            throws IOException;
}
