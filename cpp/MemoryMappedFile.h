/*
 * Copyright 2019 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#ifndef MemoryMappedFile_H
#define MemoryMappedFile_H

#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

using namespace std;

typedef char byte;

class MemoryMappedFile
{
private:

    static int BYTE_ARRAY_OFFSET;

    int _fd;
    struct stat _st;
    char *_mapped;
    long _size;
    string _location;

    static inline long roundTo4096(long i)
    {
        return (i + 0xfffL) & ~0xfffL;
    }

    void mapAndSetOffset();

    

public:
    /**
     * Constructs a new memory mapped file.
     *
     * @param location the file name
     * @param size the file length
     */
    MemoryMappedFile(string &location, long size) : _location(location), _size(roundTo4096(size))
    {
        mapAndSetOffset();
    }

    ~MemoryMappedFile()
    {
        unmap();
    }

    void unmap()
    {
        munmap(_mapped, _st.st_size);
    }

    /**
     * Reads a byte from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    byte getByte(long pos)
    {
        return 0;
    }

    /**
     * Reads a byte (volatile) from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    byte getByteVolatile(long pos)
    {
        return 0;
    }

    short getShort(long pos)
    {
        return 0;
    }

    short getShortVolatile(long pos)
    {
        return 0;
    }

    /**
     * Reads an int from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    int getInt(long pos)
    {
        return 0;
    }

    /**
     * Reads an int (volatile) from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    int getIntVolatile(long pos)
    {
        return 0;
    }

    /**
     * Reads a long from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    long getLong(long pos)
    {
        return 0;
    }

    /**
     * Reads a long (volatile) from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    long getLongVolatile(long pos)
    {
        return 0;
    }

    /**
     * Writes a byte to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    void putByte(long pos, byte val)
    {
    }

    void putBytes(long pos, byte* val, int length)
    {
        for (int i = 0; i < length ; ++i)
        {
            
        }
    }

    /**
     * Writes a byte (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    void putByteVolatile(long pos, byte val)
    {
    }

    void putShort(long pos, short val)
    {
    }

    void putShortVolatile(long pos, short val)
    {
    }

    /**
     * Writes an int to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    void putInt(long pos, int val)
    {
    }

    /**
     * Writes an int (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    void putIntVolatile(long pos, int val)
    {
    }

    /**
     * Writes a long to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    void putLong(long pos, long val)
    {
    }

    /**
     * Writes a long (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    void putLongVolatile(long pos, long val)
    {
    }

    /**
     * Reads a buffer of data.
     *
     * @param pos    the position in the memory mapped file
     * @param data   the input buffer
     * @param offset the offset in the buffer of the first byte to read data into
     * @param length the length of the data
     */
    void getBytes(long pos, byte* data, int offset, int length)
    {
    }

    /**
     * Writes a buffer of data.
     *
     * @param pos    the position in the memory mapped file
     * @param data   the output buffer
     * @param offset the offset in the buffer of the first byte to write
     * @param length the length of the data
     */
    void setBytes(long pos, byte* data, int offset, int length)
    {
    }

    bool compareAndSwapInt(long pos, int expected, int value)
    {
    }

    bool compareAndSwapLong(long pos, long expected, long value)
    {
    }

    long getAndAddLong(long pos, long delta)
    {
    }

    long getSize()
    {
        return _size;
    }
};

#endif
