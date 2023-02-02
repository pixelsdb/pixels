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

import com.sun.jna.Pointer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created at: 02/02/2023
 * Author: hank
 */
public class AlignedDirectBuffer implements Closeable
{
    private ByteBuffer buffer;
    private Pointer pointer;
    private final long address;
    private final int size;
    private final int allocatedSize;

    protected AlignedDirectBuffer(Pointer alignedPointer, int size, int allocatedSize) throws IllegalAccessException
    {
        this.pointer = alignedPointer;
        this.size = size;
        this.allocatedSize = allocatedSize;
        this.address = DirectIoLib.getAddress(alignedPointer);
        this.buffer = DirectIoLib.newDirectByteBufferR(allocatedSize, this.address);
    }

    public void shift(int pos)
    {
        checkArgument(pos + this.size <= this.allocatedSize,
                "shift leads to truncation which is not allowed");
        this.buffer.position(pos);
        this.buffer.limit(pos + this.size);
    }

    public void reset()
    {
        this.buffer.clear();
    }

    public ByteBuffer getBuffer()
    {
        return buffer;
    }

    public int getSize()
    {
        return size;
    }

    public int getAllocatedSize()
    {
        return allocatedSize;
    }

    public Pointer getPointer()
    {
        return pointer;
    }

    public long getAddress()
    {
        return address;
    }

    public boolean hasRemaining()
    {
        return this.buffer.hasRemaining();
    }

    public int remaining()
    {
        return this.buffer.remaining();
    }

    public byte get()
    {
        return this.buffer.get();
    }

    public short getShort()
    {
        return this.buffer.getShort();
    }

    public char getChar()
    {
        return this.buffer.getChar();
    }

    public int getInt()
    {
        return this.buffer.getInt();
    }

    public long getLong()
    {
        return this.buffer.getLong();
    }

    public float getFloat()
    {
        return this.buffer.getFloat();
    }

    public double getDouble()
    {
        return this.buffer.getDouble();
    }

    public int position()
    {
        return this.buffer.position();
    }

    @Override
    public void close() throws IOException
    {
        DirectIoLib.free(this.pointer);
        this.pointer = null;
        this.buffer = null;
    }
}
