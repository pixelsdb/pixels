package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;

import java.io.Closeable;
import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface PixelsWriter
        extends Closeable
{
    /**
     * add row batch into the file
     *
     * @return if the file adds a new row group, return false. Else, return true.
     */
    boolean addRowBatch(VectorizedRowBatch rowBatch)
            throws IOException;

    /**
     * Get schema of this file
     *
     * @return schema
     */
    TypeDescription getSchema();
}
