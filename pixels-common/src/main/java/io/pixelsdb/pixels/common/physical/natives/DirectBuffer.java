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

import com.sun.jna.Pointer;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The buffer that is allocated in off-heap memory. We need more controls on the buffer,
 * so we do not used jdk's DirectByteBuffer.
 * <br/>
 * Created at: 02/02/2023
 * Author: hank
 */
public class DirectBuffer implements Closeable
{
    private ByteBuffer buffer;
    private Pointer pointer;
    private final long address;
    private final int size;
    private final int allocatedSize;
    private final boolean jvmManaged;

    /**
     * Create a direct buffer from a pointer to the allocated direct memory.
     * @param alignedPointer the pointer to the allocated memory.
     * @param size the size of the valid window.
     * @param allocatedSize the allocated size of the buffer.
     * @param jvmManaged whether the allocated memory is managed by JVM. If it is <b>TRUE</b>,
     *                   the {@link #close()} method on this buffer is not responsible for freeing the memory.
     * @throws IllegalAccessException
     */
    protected DirectBuffer(Pointer alignedPointer, int size, int allocatedSize, boolean jvmManaged) throws IllegalAccessException
    {
        this.pointer = alignedPointer;
        this.size = size;
        this.allocatedSize = allocatedSize;
        this.address = DirectIoLib.getAddress(alignedPointer);
        this.buffer = DirectIoLib.wrapReadOnlyDirectByteBuffer(allocatedSize, this.address);
        this.jvmManaged = jvmManaged;
    }

    /**
     * Create a direct buffer from an allocated direct byte buffer.
     * @param buffer the allocated direct byte buffer.
     * @param size the size (i.e., capacity) of the direct byte buffer.
     * @param jvmManaged whether the allocated memory is managed by JVM. If it is <b>TRUE</b>,
     *                   the {@link #close()} method on this buffer is not responsible for freeing the memory.
     * @throws IllegalAccessException
     */
    protected DirectBuffer(ByteBuffer buffer, int size, boolean jvmManaged) throws InvocationTargetException, IllegalAccessException
    {
        checkArgument(buffer.isDirect(), "buffer must be direct");
        checkArgument(buffer.capacity() >= size, "the byte buffer is smaller than the given size");
        this.buffer = buffer.isReadOnly() ? buffer : buffer.asReadOnlyBuffer();
        this.address = DirectIoLib.getAddress(this.buffer);
        this.pointer = Pointer.createConstant(this.address);
        this.size = size;
        this.allocatedSize = size;
        this.jvmManaged = jvmManaged;
    }

    /**
     * Shift the valid window in this buffer. This method should only be used if this buffer is aligned.
     * @param startPosition the new starting position of the valid window.
     */
    public void shift(int startPosition)
    {
        checkArgument(startPosition >= 0 && startPosition + this.size <= this.allocatedSize,
                "shift leads to truncation which is not allowed");
        this.buffer.clear();
        this.buffer.position(startPosition);
        this.buffer.limit(startPosition + this.size);
    }

    /**
     * Push the position of this buffer forward by delta bytes.
     * @param delta the number of bytes, can be negative.
     */
    public void forward(int delta)
    {
        checkArgument(this.buffer.position() + delta >= 0 &&
                        this.buffer.position() + delta <= this.buffer.limit(),
                "forward out of limit is not allowed");
        this.buffer.position(this.buffer.position() + delta);
    }

    public void reset()
    {
        this.buffer.clear();
    }

    public boolean isJvmManaged()
    {
        return this.jvmManaged;
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
        if (!jvmManaged)
        {
            // free the memory if this buffer is not managed by JVM
            DirectIoLib.free(this.pointer);
        }
        this.pointer = null;
        this.buffer = null;
    }
}
