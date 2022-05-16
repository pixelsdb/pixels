/*
 * Copyright 2018 PixelsDB.
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

/*
 * This file is derived from MemoryMappedFile in MappedBus,
 * with the attribution notice:
 *
 *   Copyright 2015 Caplogic AB.
 *   Licensed under the Apache License, Version 2.0.
 *   This class was inspired from an entry in Bryce Nyeggen's blog.
 *
 * We changed the visibility of some methods from protect to public,
 * and added direct (i.e. zero-copy) memory access.
 */
package io.pixelsdb.pixels.cache;

import sun.nio.ch.FileChannelImpl;

import java.io.FileDescriptor;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static io.pixelsdb.pixels.common.utils.JvmUtils.unsafe;
import static io.pixelsdb.pixels.common.utils.JvmUtils.nativeOrder;

/**
 * This class has been tested.
 * It can read and write memory mapped file larger than 2GB.
 * When the backing file is located under /dev/shm/, it works as a shared memory,
 * and the random read/write latency is around 100ns.
 *
 * @author hank
 */

@SuppressWarnings("restriction")
public class MemoryMappedFile
{
    private static final Method mmap;
    private static final Method unmmap;
    // this is from sun.nio.ch.Util
    private static volatile Constructor<?> directByteBufferRConstructor = null;
    private static final int BYTE_ARRAY_OFFSET;

    private long addr, size;
    private final String loc;

    static
    {
        try
        {
            mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
            BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        // this is from sun.nio.ch.Util.initDBBRConstructor
        try
        {
            Class<?> cl = Class.forName("java.nio.DirectByteBufferR");
            Constructor<?> ctor = cl.getDeclaredConstructor(
                    new Class<?>[] { int.class, long.class, FileDescriptor.class, Runnable.class } );
            ctor.setAccessible(true);
            directByteBufferRConstructor = ctor;
        } catch (ClassNotFoundException |
                NoSuchMethodException |
                IllegalArgumentException |
                ClassCastException e)
        {
            throw new InternalError(e);
        }
    }

    private static Method getMethod(Class<?> cls, String name, Class<?>... params)
            throws Exception
    {
        Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    // this is derived from sun.nio.ch.Util.newMappedByteBufferR
    // create a read only direct byte buffer without memory copy.
    static ByteBuffer newDirectByteBufferR(int size, long addr)
    {
        ByteBuffer buffer;
        try
        {
            buffer = (ByteBuffer) directByteBufferRConstructor.newInstance(
                    new Object[]{ size, addr, null, null });
        } catch (InstantiationException |
                IllegalAccessException |
                InvocationTargetException e)
        {
            throw new InternalError(e);
        }
        return buffer;
    }

    private static long roundTo4096(long i)
    {
        return (i + 0xfffL) & ~0xfffL;
    }

    private void mapAndSetOffset()
            throws Exception
    {
        final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw");
        backingFile.setLength(this.size);
        final FileChannel ch = backingFile.getChannel();
        this.addr = (long) mmap.invoke(ch, 1, 0L, this.size);
        ch.close();
        backingFile.close();
    }

    /**
     * Constructs a new memory mapped file.
     *
     * @param loc the file name
     * @param len the file length
     * @throws Exception in case there was an error creating the memory mapped file
     */
    public MemoryMappedFile(final String loc, long len)
            throws Exception
    {
        this.loc = loc;
        this.size = roundTo4096(len);
        mapAndSetOffset();
    }

    public void unmap()
            throws Exception
    {
        unmmap.invoke(null, addr, this.size);
    }

    public void clear()
    {
        unsafe.setMemory(addr, size, (byte)0);
    }

    public static ByteOrder getOrder()
    {
        return nativeOrder;
    }

    /**
     * Reads a byte from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public byte getByte(long pos)
    {
        return unsafe.getByte(pos + addr);
    }

    /**
     * Reads a byte (volatile) from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public byte getByteVolatile(long pos)
    {
        return unsafe.getByteVolatile(null, pos + addr);
    }

    public short getShort(long pos)
    {
        return unsafe.getShort(pos + addr);
    }

    public short getShortVolatile(long pos)
    {
        return unsafe.getShortVolatile(null, pos + addr);
    }

    /**
     * Reads an int from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public int getInt(long pos)
    {
        return unsafe.getInt(pos + addr);
    }

    /**
     * Reads an int (volatile) from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public int getIntVolatile(long pos)
    {
        return unsafe.getIntVolatile(null, pos + addr);
    }

    /**
     * Reads a long from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLong(long pos)
    {
        return unsafe.getLong(pos + addr);
    }

    /**
     * Reads a long (volatile) from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLongVolatile(long pos)
    {
        return unsafe.getLongVolatile(null, pos + addr);
    }

    /**
     * Writes a byte to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setByte(long pos, byte val)
    {
        unsafe.putByte(pos + addr, val);
    }

    /**
     * Writes a byte (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setByteVolatile(long pos, byte val)
    {
        unsafe.putByteVolatile(null, pos + addr, val);
    }

    public void setShort(long pos, short val)
    {
        unsafe.putShort(pos + addr, val);
    }

    public void setShortVolatile(long pos, short val)
    {
        unsafe.putShortVolatile(null, pos + addr, val);
    }

    /**
     * Writes an int to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setInt(long pos, int val)
    {
        unsafe.putInt(pos + addr, val);
    }

    /**
     * Writes an int (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setIntVolatile(long pos, int val)
    {
        unsafe.putIntVolatile(null, pos + addr, val);
    }

    /**
     * Writes a long to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setLong(long pos, long val)
    {
        unsafe.putLong(pos + addr, val);
    }

    /**
     * Writes a long (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setLongVolatile(long pos, long val)
    {
        unsafe.putLongVolatile(null, pos + addr, val);
    }

    /**
     * Reads a buffer of data, with memory copy.
     *
     * @param pos    the position in the memory mapped file
     * @param data   the input buffer
     * @param offset the offset in the buffer of the first byte to read data into
     * @param length the length of the data
     */
    public void getBytes(long pos, byte[] data, int offset, int length)
    {
        unsafe.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET + offset, length);
    }

    public void getBytes(long pos, byte[] data) {
        getBytes(pos, data, 0, data.length);
    }

    /**
     * Get a direct byte buffer of data without memory copy.
     * The returned byte buffer is read only. Any read (get)
     * methods will be performed on the mapped memory (this.data)
     * directly, just as the read (get) methods in MemoryMappedFile.
     *
     * @param pos    the position in the memory mapped file
     * @param length the length of the data
     * @return the direct byte buffer, which is read only
     */
    public ByteBuffer getDirectByteBuffer(long pos, int length)
    {
        return newDirectByteBufferR(length, pos + addr);
    }

    /**
     * Writes a buffer of data.
     *
     * @param pos    the position in the memory mapped file
     * @param data   the output buffer
     * @param offset the offset in the buffer of the first byte to write
     * @param length the length of the data to write
     */
    public void setBytes(long pos, byte[] data, int offset, int length)
    {
        unsafe.copyMemory(data, BYTE_ARRAY_OFFSET + offset, null, pos + addr, length);
    }

    /**
     * Writes a buffer of data.
     *
     * @param pos    the position in the memory mapped file
     * @param data   the output buffer
     */
    public void setBytes(long pos, byte[] data)
    {
        unsafe.copyMemory(data, BYTE_ARRAY_OFFSET, null, pos + addr, data.length);
    }

    public void copyMemory(long srcPos, long destPos, long length)
    {
        unsafe.copyMemory(srcPos, destPos, length);
    }

    public boolean compareAndSwapInt(long pos, int expected, int value)
    {
        return unsafe.compareAndSwapInt(null, pos + addr, expected, value);
    }

    public boolean compareAndSwapLong(long pos, long expected, long value)
    {
        return unsafe.compareAndSwapLong(null, pos + addr, expected, value);
    }

    public long getAndAddLong(long pos, long delta)
    {
        return unsafe.getAndAddLong(null, pos + addr, delta);
    }

    public long getAndAddInt(long pos, int delta)
    {
        return unsafe.getAndAddInt(null, pos + addr, delta);
    }

    public long getSize()
    {
        return size;
    }

    public long getAddress() { return addr; }

}