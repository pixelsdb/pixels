package io.pixelsdb.pixels.common.physical.natives;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Random access file in pixels, currently it is read-only.
 * We assume that all the data in this file were written in big endian.
 *
 * @author hank
 * @create 2023-02-21
 */
public interface PixelsRandomAccessFile extends DataInput, Closeable
{
    void seek(long offset) throws IOException;

    long length();

    /**
     * Read a number of bytes starting from the current offset.
     * @param len the number of bytes to read.
     * @return the bytes read from the file.
     * @throws IOException
     */
    ByteBuffer readFully(int len) throws IOException;

    /**
     * Read a number of bytes starting from the given offset. This method does not change the current file offset.
     * @param off the given offset.
     * @param len the number of bytes to read.
     * @return the bytes read from the file.
     * @throws IOException
     */
    ByteBuffer readFully(long off, int len) throws IOException;

    @Override
    default String readLine() throws IOException
    {
        throw new UnsupportedOperationException("read line is not supported");
    }

    @Override
    default String readUTF() throws IOException
    {
        throw new UnsupportedOperationException("read UTF is not supported");
    }
}
