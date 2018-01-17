package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;

import java.io.Closeable;
import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public abstract class PixelsWriter
        implements Closeable
{
    public abstract void addRowBatch(VectorizedRowBatch rowBatch) throws IOException;
}
