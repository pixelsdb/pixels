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
     * @param batch the row batch to read into
     * @return more rows available
     * @throws IOException
     * */
    boolean readBatch(VectorizedRowBatch batch) throws IOException;

    /**
     * Read the next row batch
     * @return row batch
     * @throws IOException
     * */
    VectorizedRowBatch readBatch() throws IOException;

    /**
     * Read the next row batch
     * @param max max num of rows in this batch
     * @return row batch
     * @throws IOException
     * */
    VectorizedRowBatch readBatch(int max) throws IOException;

    /**
     * Get current row number
     * @return number of the row currently being read
     * */
    long getRowNumber();

    /**
     * Seek to specified row
     * @param rowIndex row index
     * @return seek success
     * @throws IOException
     * */
    @Deprecated
    boolean seekToRow(long rowIndex) throws IOException;

    /**
     * Skip specified number of rows
     *
     * @param rowNum number of rows to skip
     * @return skip success
     * @throws IOException
     * */
    @Deprecated
    boolean skip(long rowNum) throws IOException;

    /**
     * Cleanup and release resources
     * @throws IOException
     * */
    @Override
    void close() throws IOException;
}
