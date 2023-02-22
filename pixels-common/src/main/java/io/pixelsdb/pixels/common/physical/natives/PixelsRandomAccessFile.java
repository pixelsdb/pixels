package io.pixelsdb.pixels.common.physical.natives;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Random access file in pixels, currently it is read-only.
 * We assume that all the data in this file were written in big endian.
 * Created at: 2/21/23
 * Author: hank
 */
public interface PixelsRandomAccessFile extends DataInput, Closeable
{
    void seek(long offset) throws IOException;

    long length();

    ByteBuffer readFully(int len) throws IOException;

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
