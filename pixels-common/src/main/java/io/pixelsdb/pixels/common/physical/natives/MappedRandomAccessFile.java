/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.natives;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.utils.JvmUtils.nativeIsLittleEndian;
import static io.pixelsdb.pixels.common.utils.JvmUtils.nativeOrder;

/**
 * Created at: 2/21/23
 * Author: hank
 */
public class MappedRandomAccessFile implements PixelsRandomAccessFile
{
    private File file;
    private long offset;
    private long length;
    private MemoryMappedFile mmf;

    public MappedRandomAccessFile(File file) throws IOException
    {
        this.file = file;
        this.offset = 0;
        this.length = this.file.length();
        this.mmf = new MemoryMappedFile(file.getPath(), this.length, false);
    }

    @Override
    public ByteBuffer readFully(int len) throws IOException
    {
        return this.mmf.getDirectByteBuffer(this.offset, len);
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        this.mmf.getBytes(this.offset, b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        this.mmf.getBytes(this.offset, b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        if (n <= 0)
        {
            return 0;
        }
        long off = this.offset;
        long newOff = Math.min(off + n, length);
        seek(newOff);
        return (int) (newOff - off);
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return this.mmf.getByte(this.offset) != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        return this.mmf.getByte(this.offset);
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return this.mmf.getByte(this.offset) & 0xff;
    }

    @Override
    public short readShort() throws IOException
    {
        if (nativeIsLittleEndian)
        {
            return Short.reverseBytes(this.mmf.getShort(this.offset));
        }
        return this.mmf.getShort(this.offset);
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        if (nativeIsLittleEndian)
        {
            return Short.reverseBytes(this.mmf.getShort(this.offset)) & 0xffff;
        }
        return this.mmf.getShort(this.offset) & 0xffff;
    }

    @Override
    public char readChar() throws IOException
    {
        return this.mmf.getChar(this.offset);
    }

    @Override
    public int readInt() throws IOException
    {
        if (nativeIsLittleEndian)
        {
            return Integer.reverseBytes(this.mmf.getInt(this.offset));
        }
        return this.mmf.getInt(this.offset);
    }

    @Override
    public long readLong() throws IOException
    {
        if (nativeIsLittleEndian)
        {
            return Long.reverseBytes(this.mmf.getLong(this.offset));
        }
        return this.mmf.getLong(this.offset);
    }

    @Override
    public float readFloat() throws IOException
    {
        if (nativeIsLittleEndian)
        {
            return this.mmf.getDirectByteBuffer(this.offset, Float.BYTES).order(nativeOrder).getFloat();
        }
        return this.mmf.getFloat(this.offset);
    }

    @Override
    public double readDouble() throws IOException
    {
        if (nativeIsLittleEndian)
        {
            return this.mmf.getDirectByteBuffer(this.offset, Double.BYTES).order(nativeOrder).getDouble();
        }
        return this.mmf.getDouble(this.offset);
    }

    @Override
    public void close() throws IOException
    {
        this.offset = 0;
        this.length = 0;
        this.file = null;
        this.mmf.unmap();
        this.mmf = null;
    }

    @Override
    public void seek(long offset) throws IOException
    {
        checkArgument(offset >= 0 && offset < this.length, "illegal offset:" + offset);
        this.offset = offset;
    }

    @Override
    public long length()
    {
        return this.length;
    }
}
