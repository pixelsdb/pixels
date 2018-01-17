package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface VectorReader
{
    /**
     * Read the next row batch.
     * The size of the batch to read cannot be controlled by the callers.
     * Caller must look at VectorizedRowBatch.size() to know the batch size read.
     *
     * @param batch a row batch object to read into
     * @return were more rows available to read?
     * @throws IOException io exception
     * */
    boolean nextBatch(VectorizedRowBatch batch) throws IOException;

    /**
     * Get the row number of the row that will be returned by the following call to next().
     * @return the row number from 0 th the number of rows in the file
     * @throws IOException io exception
     * */
    long getRowNumber() throws IOException;

    /**
     * Release the resources associated with the given reader
     * @throws IOException io exception
     * */
    void close() throws IOException;
}
