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
package io.pixelsdb.pixels.common.physical.direct;

import java.io.Closeable;
import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * The random accessible file that can be opened with o_direct flag.
 * Currently, it is readonly.
 *
 * Created at: 02/02/2023
 * Author: hank
 */
public class DirectRandomAccessFile implements DataInput, Closeable
{
    private File file;
    private int fd;
    private long offset;
    private long length;
    private final int blockSize;
    private boolean bufferValid;
    private DirectBuffer smallBuffer;
    private LinkedList<DirectBuffer> largeBuffers = new LinkedList<>();

    public DirectRandomAccessFile(File file, boolean readOnly) throws IOException
    {
        this.file = file;
        this.fd = DirectIoLib.open(file.getPath(), readOnly);
        this.offset = 0;
        this.length = this.file.length();
        this.blockSize = DirectIoLib.fsBlockSize;
        this.bufferValid = false;
        try
        {
            this.smallBuffer = DirectIoLib.allocateBuffer(DirectIoLib.fsBlockSize);
        } catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new IOException("failed to allocate buffer", e);
        }
    }

    @Override
    public void close() throws IOException
    {
        this.smallBuffer.close();
        this.smallBuffer = null;
        for (DirectBuffer largeBuffer : this.largeBuffers)
        {
            largeBuffer.close();
        }
        this.largeBuffers.clear();
        this.largeBuffers = null;
        DirectIoLib.close(fd);
        this.fd = -1;
        this.offset = 0;
        this.length = 0;
        this.file = null;
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        ByteBuffer buffer = readFully(b.length);
        buffer.get(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        ByteBuffer buffer = readFully(len);
        buffer.get(b, off, len);
    }

    public ByteBuffer readFully(int len) throws IOException
    {
        try
        {
            DirectBuffer buffer = DirectIoLib.allocateBuffer(len);
            DirectIoLib.read(this.fd, this.offset, buffer, len);
            this.seek(this.offset + len);
            this.largeBuffers.add(buffer);
            return buffer.getBuffer();
        } catch (IllegalAccessException | InvocationTargetException e)
        {
            throw new IOException("failed to allocate buffer", e);
        }
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
        return (int)(newOff - off);
    }

    private void populateBuffer() throws IOException
    {
        DirectIoLib.read(this.fd, this.offset, this.smallBuffer, this.blockSize);
        this.bufferValid = true;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.hasRemaining())
        {
            this.populateBuffer();
        }
        this.offset++;
        return this.smallBuffer.get() != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.hasRemaining())
        {
            this.populateBuffer();
        }
        this.offset++;
        return this.smallBuffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.hasRemaining())
        {
            this.populateBuffer();
        }
        this.offset++;
        return this.smallBuffer.get();
    }

    @Override
    public short readShort() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.remaining() < Short.BYTES)
        {
            this.populateBuffer();
        }
        this.offset += Short.BYTES;
        return this.smallBuffer.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.remaining() < Short.BYTES)
        {
            this.populateBuffer();
        }
        int b1 = this.smallBuffer.get();
        int b2 = this.smallBuffer.get();
        this.offset += 2;
        return (b1<<8) + b2;
    }

    @Override
    public char readChar() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.remaining() < Character.BYTES)
        {
            this.populateBuffer();
        }
        this.offset += Character.BYTES;
        return this.smallBuffer.getChar();
    }

    @Override
    public int readInt() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.remaining() < Integer.BYTES)
        {
            this.populateBuffer();
        }
        this.offset += Integer.BYTES;
        return this.smallBuffer.getInt();
    }

    @Override
    public long readLong() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.remaining() < Long.BYTES)
        {
            this.populateBuffer();
        }
        this.offset += Long.BYTES;
        return this.smallBuffer.getLong();
    }

    @Override
    public float readFloat() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.remaining() < Float.BYTES)
        {
            this.populateBuffer();
        }
        this.offset += Float.BYTES;
        return this.smallBuffer.getFloat();
    }

    @Override
    public double readDouble() throws IOException
    {
        if (!this.bufferValid || this.smallBuffer.remaining() < Double.BYTES)
        {
            this.populateBuffer();
        }
        this.offset += Double.BYTES;
        return this.smallBuffer.getDouble();
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException("read line is not supported");
    }

    /**
     * This method is currently not implemented.
     */
    @Override
    public String readUTF() throws IOException
    {
        throw new UnsupportedOperationException("read UTF is not supported");
    }

    public void seek(long offset) throws IOException
    {
        if (this.bufferValid && offset > this.offset - this.smallBuffer.position() &&
                offset < this.offset + this.smallBuffer.remaining())
        {
            this.smallBuffer.forward((int) (offset - this.offset));
        }
        else
        {
            this.bufferValid = false;
        }
        this.offset = offset;
    }

    public long length()
    {
        return this.length;
    }
}
