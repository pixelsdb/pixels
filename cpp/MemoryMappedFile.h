#ifndef MemoryMappedFile_H
#define MemoryMappedFile_H

#include <string>

using namespace std;

typedef char byte;

class MemoryMappedFile
{
private:
    //Unsafe unsafe;
    //private static final Method mmap;
    //Method unmmap;
    static int BYTE_ARRAY_OFFSET;

    long addr, size;
    string loc;

    static long roundTo4096(long i)
    {
        return (i + 0xfffL) & ~0xfffL;
    }

    void mapAndSetOffset()
    {
    }

public:
    /**
     * Constructs a new memory mapped file.
     *
     * @param loc the file name
     * @param len the file length
     * @throws Exception in case there was an error creating the memory mapped file
     */
    MemoryMappedFile(string &loc, long len)
    {
        loc = loc;
        size = roundTo4096(len);
        mapAndSetOffset();
    }

    void unmap()
    {
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
        return size;
    }
};

#endif
