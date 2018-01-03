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
    /**
     * Prepare the writer to ensure the length can fit into current hdfs block
     * @param length length of content
     * @return starting offset after preparing. If -1, means prepare failed. The specified length can not fit
     * */
    long prepare(int length) throws IOException;

    long append(ByteBuffer buffer) throws IOException;

    long append(byte[] buffer, int offset, int length) throws IOException;

    void close() throws IOException;

    void flush() throws IOException;
}
