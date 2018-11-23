/*
* Copyright 2015 Caplogic AB.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/*
* This class was inspired from an entry in Bryce Nyeggen's blog 
*/
package cn.edu.ruc.iir.pixels.cache;
import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

/**
 * Comment added by Pixels author:
 * This class has been tested.
 * It can read and write memory mapped file which is larger than 2GB.
 * When the backing file is located under /dev/shm/, it works as a shared memory.
 * The random read/write latency is sub-nanosecond.
 *
 * Other modifications:
 * change the visibility of some methods from protect to public
 */

@SuppressWarnings("restriction")
public class MemoryMappedFile
{
    private static final Unsafe unsafe;
    private static final Method mmap;
    private static final Method unmmap;
    private static final int BYTE_ARRAY_OFFSET;
 
    private long addr, size;
    private final String loc;
 
    static
    {
        try {
            Field singleOneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleOneInstanceField.setAccessible(true);
            unsafe = (Unsafe) singleOneInstanceField.get(null);
            mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
            BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception
    {
        Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }
 
    private static long roundTo4096(long i)
    {
        return (i + 0xfffL) & ~0xfffL;
    }
 
    private void mapAndSetOffset() throws Exception
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
     * @param loc the file name
     * @param len the file length
     * @throws Exception in case there was an error creating the memory mapped file
     */
    public MemoryMappedFile(final String loc, long len) throws Exception
    {
        this.loc = loc;
        this.size = roundTo4096(len);
        mapAndSetOffset();
    }

    public void unmap() throws Exception
    {
        unmmap.invoke(null, addr, this.size);
    }
    
    /**
     * Reads a byte from the specified position.
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public byte getByte(long pos)
    {
        return unsafe.getByte(pos + addr);
    }

    /**
     * Reads a byte (volatile) from the specified position.
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
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public int getInt(long pos)
    {
        return unsafe.getInt(pos + addr);
    }

    /**
     * Reads an int (volatile) from the specified position.
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public int getIntVolatile(long pos)
    {
        return unsafe.getIntVolatile(null, pos + addr);
    }

    /**
     * Reads a long from the specified position.
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLong(long pos)
    {
        return unsafe.getLong(pos + addr);
    }
    
    /**
     * Reads a long (volatile) from the specified position.
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLongVolatile(long pos)
    {
        return unsafe.getLongVolatile(null, pos + addr);
    }
    
    /**
     * Writes a byte to the specified position.
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putByte(long pos, byte val)
    {
        unsafe.putByte(pos + addr, val);
    }

    public void putBytes(long pos, byte[] val)
    {
        for (byte v : val) {
            unsafe.putByte(pos++ + addr, v);
        }
    }
    
    /**
     * Writes a byte (volatile) to the specified position.
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putByteVolatile(long pos, byte val)
    {
        unsafe.putByteVolatile(null, pos + addr, val);
    }

    public void putShort(long pos, short val)
    {
        unsafe.putShort(pos + addr, val);
    }

    public void putShortVolatile(long pos, short val)
    {
        unsafe.putShortVolatile(null, pos + addr, val);
    }

    /**
     * Writes an int to the specified position.
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putInt(long pos, int val)
    {
        unsafe.putInt(pos + addr, val);
    }

    /**
     * Writes an int (volatile) to the specified position.
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putIntVolatile(long pos, int val)
    {
        unsafe.putIntVolatile(null, pos + addr, val);
    }

    /**
     * Writes a long to the specified position.
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putLong(long pos, long val)
    {
        unsafe.putLong(pos + addr, val);
    }
    
    /**
     * Writes a long (volatile) to the specified position.
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putLongVolatile(long pos, long val)
    {
        unsafe.putLongVolatile(null, pos + addr, val);
    }
    
    /**
     * Reads a buffer of data.
     * @param pos the position in the memory mapped file
     * @param data the input buffer
     * @param offset the offset in the buffer of the first byte to read data into
     * @param length the length of the data
     */
    public void getBytes(long pos, byte[] data, int offset, int length)
    {
        unsafe.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET + offset, length);
    }
 
    /**
     * Writes a buffer of data.
     * @param pos the position in the memory mapped file
     * @param data the output buffer
     * @param offset the offset in the buffer of the first byte to write
     * @param length the length of the data
     */
    public void setBytes(long pos, byte[] data, int offset, int length)
    {
        unsafe.copyMemory(data, BYTE_ARRAY_OFFSET + offset, null, pos + addr, length);
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
}