package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface PixelsRecordReader
        extends AutoCloseable
{
    /**
     * Read the next row batch.
     * @param batchSize the row batch size
     * @return vectorized row batch
     * @throws java.io.IOException
     * */
    VectorizedRowBatch readBatch(int batchSize) throws IOException;

    /**
     * Read the next row batch
     * @return row batch
     * @throws java.io.IOException
     * */
    VectorizedRowBatch readBatch() throws IOException;

    /**
     * Get current row number
     * @return number of the row currently being read
     * */
    long getRowNumber();

    /**
     * Seek to specified row
     * @param rowIndex row index
     * @return seek success
     * @throws java.io.IOException
     * */
    @Deprecated
    boolean seekToRow(long rowIndex) throws IOException;

    /**
     * Skip specified number of rows
     *
     * @param rowNum number of rows to skip
     * @return skip success
     * @throws java.io.IOException
     * */
    @Deprecated
    boolean skip(long rowNum) throws IOException;

    long getCompletedBytes();

    /**
     * Cleanup and release resources
     * @throws java.io.IOException
     * */
    @Override
    void close() throws IOException;
}
