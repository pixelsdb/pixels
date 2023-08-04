/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.encoding;

import io.pixelsdb.pixels.core.exception.PixelsEncodingException;

import java.io.IOException;

/**
 * @author guodong
 * @author hank
 */
public abstract class Encoder implements AutoCloseable
{
    public byte[] encode(boolean[] values)
    {
        throw new PixelsEncodingException("Encoding boolean values is not supported");
    }

    public byte[] encode(boolean[] values, long offset, long length)
    {
        throw new PixelsEncodingException("Encoding boolean values is not supported");
    }

    public byte[] encode(char[] values)
    {
        throw new PixelsEncodingException("Encoding char values is not supported");
    }

    public byte[] encode(char[] values, int offset, int length)
    {
        throw new PixelsEncodingException("Encoding char values is not supported");
    }

    public byte[] encode(byte[] values) throws IOException
    {
        throw new PixelsEncodingException("Encoding byte values is not supported");
    }

    public byte[] encode(byte[] values, int offset, int length) throws IOException
    {
        throw new PixelsEncodingException("Encoding byte values is not supported");
    }

    public byte[] encode(long[] values) throws IOException
    {
        throw new PixelsEncodingException("Encoding long values is not supported");
    }

    public byte[] encode(long[] values, int offset, int length) throws IOException
    {
        throw new PixelsEncodingException("Encoding long values is not supported");
    }

    public byte[] encode(int[] values) throws IOException
    {
        throw new PixelsEncodingException("Encoding int values is not supported");
    }

    public byte[] encode(int[] values, int offset, int length) throws IOException
    {
        throw new PixelsEncodingException("Encoding int values is not supported");
    }

    public byte[] encode(short[] values)
    {
        throw new PixelsEncodingException("Encoding short values is not supported");
    }

    public byte[] encode(float[] values)
    {
        throw new PixelsEncodingException("Encoding float values is not supported");
    }

    public byte[] encode(double[] values)
    {
        throw new PixelsEncodingException("Encoding double values is not supported");
    }

    abstract public void close() throws IOException;
}
