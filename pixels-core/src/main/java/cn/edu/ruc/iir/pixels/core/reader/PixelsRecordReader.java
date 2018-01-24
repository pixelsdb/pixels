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
     * @throws java.io.IOException
     * */
    boolean nextBatch(VectorizedRowBatch batch) throws IOException;

    /**
     * Get current row number
     * @return number of the row currently being read
     * @throws java.io.IOException
     * */
    long getRowNumber() throws IOException;

    /**
     * Seek to specified row
     * @param rowNumber row number
     * @return seek success
     * @throws java.io.IOException
     * */
    boolean seekToRow(long rowNumber) throws IOException;

    /**
     * Cleanup and release resources
     * @throws java.io.IOException
     * */
    @Override
    void close() throws IOException;
}
