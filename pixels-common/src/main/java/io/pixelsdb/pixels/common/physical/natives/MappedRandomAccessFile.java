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
 * @author hank
 * @create 2023-02-21
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
        if (len <= 0 || this.offset + len > this.length)
        {
            throw new IOException(String.format("illegal length (%d)", len));
        }
        ByteBuffer byteBuffer = this.mmf.getDirectByteBuffer(this.offset, len);
        this.offset += len;
        return byteBuffer;
    }

    @Override
    public ByteBuffer readFully(long off, int len) throws IOException
    {
        if (off < 0 || len <= 0 || off + len > this.length)
        {
            throw new IOException(String.format("illegal offset (%d), length (%d)", off, len));
        }
        ByteBuffer byteBuffer = this.mmf.getDirectByteBuffer(off, len);
        return byteBuffer;
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        if (b == null)
        {
            throw new IOException("buffer is null");
        }
        if (b.length <= 0 || this.offset + b.length > this.length)
        {
            throw new IOException(String.format("illegal buffer size (%d)", b.length));
        }
        this.mmf.getBytes(this.offset, b);
        this.offset += b.length;
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        if (b == null)
        {
            throw new IOException("buffer is null");
        }
        if (off < 0 || len <= 0 || this.offset + len > this.length || off + len > b.length)
        {
            throw new IOException(String.format("illegal offset (%d), length (%d), or buffer size (%d)", off, len, b.length));
        }
        this.mmf.getBytes(this.offset, b, off, len);
        this.offset += len;
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
        if (this.offset >= this.length)
        {
            throw new IOException("read beyond EOF");
        }
        return this.mmf.getByte(this.offset++) != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        if (this.offset >= this.length)
        {
            throw new IOException("read beyond EOF");
        }
        return this.mmf.getByte(this.offset++);
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        if (this.offset >= this.length)
        {
            throw new IOException("read beyond EOF");
        }
        return this.mmf.getByte(this.offset++) & 0xff;
    }

    @Override
    public short readShort() throws IOException
    {
        if (this.offset + Short.BYTES > this.length)
        {
            throw new IOException("read beyond EOF");
        }
        short v;
        if (nativeIsLittleEndian)
        {
            v = Short.reverseBytes(this.mmf.getShort(this.offset));
        } else
        {
            v = this.mmf.getShort(this.offset);
        }
        this.offset += Short.BYTES;
        return v;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        if (this.offset + Short.BYTES > this.length)
        {
            throw new IOException("read beyond EOF");
        }
        int v;
        if (nativeIsLittleEndian)
        {
            v = Short.reverseBytes(this.mmf.getShort(this.offset)) & 0xffff;
        } else
        {
            v = this.mmf.getShort(this.offset) & 0xffff;
        }
        this.offset += Short.BYTES;
        return v;
    }

    @Override
    public char readChar() throws IOException
    {
        if (this.offset + Character.BYTES > this.length)
        {
            throw new IOException("read beyond EOF");
        }
        char v = this.mmf.getChar(this.offset);
        this.offset += Character.BYTES;
        return v;
    }

    @Override
    public int readInt() throws IOException
    {
        if (this.offset + Integer.BYTES > this.length)
        {
            throw new IOException("read beyond EOF");
        }
        int v;
        if (nativeIsLittleEndian)
        {
            v = Integer.reverseBytes(this.mmf.getInt(this.offset));
        } else
        {
            v = this.mmf.getInt(this.offset);
        }
        this.offset += Integer.BYTES;
        return v;
    }

    @Override
    public long readLong() throws IOException
    {
        if (this.offset + Long.BYTES > this.length)
        {
            throw new IOException("read beyond EOF");
        }
        long v;
        if (nativeIsLittleEndian)
        {
            v = Long.reverseBytes(this.mmf.getLong(this.offset));
        } else
        {
            v = this.mmf.getLong(this.offset);
        }
        this.offset += Long.BYTES;
        return v;
    }

    @Override
    public float readFloat() throws IOException
    {
        if (this.offset + Float.BYTES > this.length)
        {
            throw new IOException("read beyond EOF");
        }
        float v;
        if (nativeIsLittleEndian)
        {
            v = this.mmf.getDirectByteBuffer(this.offset, Float.BYTES).order(nativeOrder).getFloat();
        } else
        {
            v = this.mmf.getFloat(this.offset);
        }
        this.offset += Float.BYTES;
        return v;
    }

    @Override
    public double readDouble() throws IOException
    {
        if (this.offset + Character.BYTES > this.length)
        {
            throw new IOException("read beyond EOF");
        }
        double v;
        if (nativeIsLittleEndian)
        {
            v = this.mmf.getDirectByteBuffer(this.offset, Double.BYTES).order(nativeOrder).getDouble();
        } else
        {
            v = this.mmf.getDouble(this.offset);
        }
        this.offset += Double.BYTES;
        return v;
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
