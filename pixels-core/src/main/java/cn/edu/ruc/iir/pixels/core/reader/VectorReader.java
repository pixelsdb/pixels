package cn.edu.ruc.iir.pixels.core.reader;

import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;

import java.io.Closeable;
import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class VectorReader
        implements Closeable
{
    /**
     * Read the next row batch.
     * The size of the batch to read cannot be controlled by the callers.
     * The caller must look at <code>VectorizedRowBatch.size()</code> to know the batch size read.
     *
     * @param batch a row batch object to read into
     * @return were more rows available to read?
     * @throws IOException io exception
     * */
    public abstract boolean nextBatch(VectorizedRowBatch batch) throws IOException;

    /**
     * Get the row number of the row that will be returned by the following call to next().
     * @return the row number from 0th to the number of rows in the file
     * @throws IOException io exception
     * */
    public abstract long getRowNumber() throws IOException;

    /**
     * Release the resources associated with the given reader
     * @throws IOException io exception
     * */
    public abstract void close() throws IOException;
}
