/*
 * Copyright 2020 PixelsDB.
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
package io.pixelsdb.pixels.core.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This input stream does not modify the current position in the backing byte buffer.
 * This class is not thread safe.
 * Created at: 20-1-1
 * Author: hank
 */
public class ByteBufferInputStream extends InputStream
{
    private ByteBuffer byteBuffer;
    private int position;
    private int limit;
    private boolean isDirect;

    /**
     * Create an input stream backed by the byteBuffer.
     * position in the backing byte buffer will not be modified.
     * @param byteBuffer the backing byte buffer.
     * @param position start offset in the byteBuffer.
     * @param limit end offset in the byteBuffer.
     */
    public ByteBufferInputStream(ByteBuffer byteBuffer, int position, int limit)
    {
        assert (byteBuffer != null);
        assert (position >=0 && position < limit);
        assert (limit <= byteBuffer.limit());
        this.byteBuffer = byteBuffer;
        this.position = position;
        this.limit = limit;
        this.isDirect = byteBuffer.isDirect();
    }

    @Override
    public int read() throws IOException
    {
        if (position >= limit)
        {
            return -1;
        }
        return byteBuffer.get(position++) & 0xff;
    }

    @Override
    public int available() throws IOException
    {
        return limit - position;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        if (position >= limit)
        {
            return -1;
        }
        if (len > (limit - position))
        {
            len = limit - position;
        }

        /**
         * Issue #377:
         * Reading direct byte buffer as following does not observe any performance gain:
         * {@code
         * long address = DirectIoLib.getAddress(byteBuffer);
         * JvmUtils.unsafe.copyMemory(null, address + position,
         *         b, ARRAY_BYTE_BASE_OFFSET + off, len);
         * position += len;
         * }
         * Therefore, we do not distinguish direct or non-direct byte buffer here.
         */
        for (int i = off; i < off + len; i++)
        {
            b[i] = byteBuffer.get(position++);
        }

        return len;
    }

    @Override
    public int read(byte[] b) throws IOException
    {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException
    {
        if (n < 0)
        {
            // Issue #374: negative n does not have any effect.
            return 0;
        }
        if (n > (limit - position))
        {
            n = limit - position;
        }
        position += (int) n;
        return n;
    }
}
