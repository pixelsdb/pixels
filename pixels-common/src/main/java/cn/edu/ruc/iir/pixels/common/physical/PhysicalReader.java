package cn.edu.ruc.iir.pixels.common.physical;

import cn.edu.ruc.iir.pixels.common.exception.FSException;

import java.io.Closeable;
import java.io.IOException;

/**
 * pixels
 *
 * @author guodong
 */
public interface PhysicalReader
        extends Closeable
{
    long getFileLength() throws IOException;

    void seek(long desired) throws IOException;

    int read(byte[] buffer) throws IOException;

    int read(byte[] buffer, int offset, int length) throws IOException;

    void readFully(byte[] buffer) throws IOException;

    void readFully(byte[] buffer, int offset, int length) throws IOException;

    long readLong() throws IOException;

    int readInt() throws IOException;

    void close() throws IOException;

    long getCurrentBlockId() throws IOException, FSException;
}
