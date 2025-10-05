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
package io.pixelsdb.pixels.common.physical.natives;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.common.physical.natives.DirectIoLib.wrapReadOnlyDirectByteBuffer;
import static io.pixelsdb.pixels.common.utils.JvmUtils.nativeOrder;
import static io.pixelsdb.pixels.common.utils.JvmUtils.unsafe;

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
    private static final int BYTE_ARRAY_OFFSET;

    private long addr;
    private final long size;
    private final String loc;

    static
    {
        BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    }

    public static long roundTo4096(long i)
    {
        return (i + 0xfffL) & ~0xfffL;
    }

    private void mapAndSetAddr(boolean forceSize) throws IOException
    {
        if (forceSize)
        {
            final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw");
            backingFile.setLength(this.size);
            backingFile.close();
        }
        try
        {
            // Issue #960: use custom mmap implementation instead of FileChannel.map for JDK compatibility.
            this.addr = DirectIoLib.mmap(this.loc, this.size, 0L, false);
            if (this.addr < 0)
            {
                throw new IOException("addr returned by the native mmap method is negative");
            }
        }
        catch (Throwable e)
        {
            throw new IOException("failed to the backing file to virtual memory", e);
        }
    }

    /**
     * Constructs a new memory mapped file. The file size will be rounded to 4KB by force, which may extend the file
     * by padding 0.
     * @param loc the file name
     * @param len the file length
     * @throws IOException in case there was an error creating the memory mapped file
     */
    public MemoryMappedFile(final String loc, long len) throws IOException
    {
        this (loc, len, true);
    }

    /**
     * Constructs a new memory mapped file.
     * @param loc the file name
     * @param len the file length
     * @param forceRound4K true to round the file length to 4KB by force, which may extend the file by padding 0.
     * @throws Exception in case there was an error creating the memory mapped file
     */
    public MemoryMappedFile(final String loc, long len, boolean forceRound4K) throws IOException
    {
        checkArgument(loc != null && !loc.isEmpty(), "loc must not be null or empty");
        checkArgument(len > 0, "len must be positive");
        this.loc = loc;
        if (forceRound4K)
        {
            this.size = roundTo4096(len);
        }
        else
        {
            this.size = len;
        }
        mapAndSetAddr(forceRound4K);
    }

    private MemoryMappedFile(final String loc, long addr, long len)
    {
        checkArgument(loc != null && !loc.isEmpty(), "loc must not be null or empty");
        checkArgument(addr >= 0, "addr must be non-negative");
        checkArgument(len > 0, "len must be positive");
        this.loc = loc;
        this.size = len;
        this.addr = addr;
    }

    /**
     * Get a view of the MemoryMappedFile given an offset in bytes.
     * @param offset the offset in the MemoryMappedFile
     * @param size the size of the view
     * @return the view
     */
    public MemoryMappedFile regionView(final long offset, final long size)
    {

        if (offset + size >= this.size)
        {
            throw new IllegalArgumentException("offset=" + offset + " plus size=" + size +
                    " is bigger than this.size=" + this.size);
        }
        return new MemoryMappedFile(this.loc, this.addr + offset, size);
    }

    public void unmap() throws IOException
    {
        try
        {
            if (addr >= 0 && this.size > 0)
            {
                DirectIoLib.munmap(this.addr, this.size);
            }
        }
        catch (Throwable e)
        {
            throw new IOException("failed to unmap the backing file from virtual memory", e);
        }
    }

    /**
     * Set all the bytes in this memory mapped file to zero. Be <b>CAREFUL</b> with this method.
     */
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

    /**
     * Reads a short from the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public short getShort(long pos)
    {
        return unsafe.getShort(pos + addr);
    }

    /**
     * Reads a short (volatile) from the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public short getShortVolatile(long pos)
    {
        return unsafe.getShortVolatile(null, pos + addr);
    }

    public char getChar(long pos)
    {
        return unsafe.getChar(pos + addr);
    }

    public char getCharVolatile(long pos)
    {
        return unsafe.getCharVolatile(null, pos + addr);
    }

    /**
     * Reads an int from the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public int getInt(long pos)
    {
        return unsafe.getInt(pos + addr);
    }

    /**
     * Reads an int (volatile) from the specified position, using native endian.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public int getIntVolatile(long pos)
    {
        return unsafe.getIntVolatile(null, pos + addr);
    }

    /**
     * Reads a long from the specified position, using native endian.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLong(long pos)
    {
        return unsafe.getLong(pos + addr);
    }

    /**
     * Reads a long (volatile) from the specified position, using native endian.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLongVolatile(long pos)
    {
        return unsafe.getLongVolatile(null, pos + addr);
    }

    /**
     * Reads a float from the specified position, using native endian.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public float getFloat(long pos)
    {
        return unsafe.getFloat(pos + addr);
    }

    /**
     * Reads a float (volatile) from the specified position, using native endian.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public float getFloatVolatile(long pos)
    {
        return unsafe.getFloatVolatile(null, pos + addr);
    }

    /**
     * Reads a double from the specified position, using native endian.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public double getDouble(long pos)
    {
        return unsafe.getDouble(pos + addr);
    }

    /**
     * Reads a double (volatile) from the specified position, using native endian.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public double getDoubleVolatile(long pos)
    {
        return unsafe.getDoubleVolatile(null, pos + addr);
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

    /**
     * Writes a short to the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setShort(long pos, short val)
    {
        unsafe.putShort(pos + addr, val);
    }

    /**
     * Writes a short (volatile) to the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setShortVolatile(long pos, short val)
    {
        unsafe.putShortVolatile(null, pos + addr, val);
    }

    /**
     * Writes an int to the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setInt(long pos, int val)
    {
        unsafe.putInt(pos + addr, val);
    }

    /**
     * Writes an int (volatile) to the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setIntVolatile(long pos, int val)
    {
        unsafe.putIntVolatile(null, pos + addr, val);
    }

    /**
     * Writes a long to the specified position, using native endian.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void setLong(long pos, long val)
    {
        unsafe.putLong(pos + addr, val);
    }

    /**
     * Writes a long (volatile) to the specified position, using native endian.
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
        return wrapReadOnlyDirectByteBuffer(length, pos + addr);
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

    /**
     * Native endian is used.
     */
    public long getAndAddLong(long pos, long delta)
    {
        return unsafe.getAndAddLong(null, pos + addr, delta);
    }

    /**
     * Native endian is used.
     */
    public long getAndAddInt(long pos, int delta)
    {
        return unsafe.getAndAddInt(null, pos + addr, delta);
    }

    public long getSize()
    {
        return size;
    }

    public String getName() {
        return loc;
    }

    public long getAddress() { return addr; }

}