package cn.edu.ruc.iir.pixels.core;

import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface PhysicalReader
{
    long getFileLength() throws IOException;

    void seek(long desired) throws IOException;

    void readFully(byte[] buffer) throws IOException;

    void readFully(byte[] buffer, int offset, int length) throws IOException;

    long readLong() throws IOException;

    void close() throws IOException;
}
