package cn.edu.ruc.iir.pixels.core;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * pixels
 *
 * @author guodong
 */
public interface PhysicalWriter
{
    long appendRowGroupBuffer(ByteBuffer buffer) throws IOException;

    void writeFileTail(PixelsProto.FileTail fileTail) throws IOException;

    void close() throws IOException;

    void flush() throws IOException;
}
