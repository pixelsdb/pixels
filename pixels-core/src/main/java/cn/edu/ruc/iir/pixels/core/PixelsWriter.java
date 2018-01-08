package cn.edu.ruc.iir.pixels.core;

import cn.edu.ruc.iir.pixels.core.vector.VectorizedRowBatch;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface PixelsWriter
{
    void addRowBatch(VectorizedRowBatch rowBatch) throws IOException;

    void close();
}
