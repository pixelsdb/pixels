/**
 ByteBuffer
 ByteBuffer.h
 Copyright 2011 - 2013 Ramsey Kant
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 Modfied 2015 by Ashley Davis (SgtCoDFish)
 */

// https://github.com/RamseyK/ByteBufferCpp

#ifndef _ByteBuffer_H_
#define _ByteBuffer_H_

// Default number of uint8_ts to allocate in the backing buffer if no size is provided
#define BB_DEFAULT_SIZE 4096

#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <vector>
#include <memory>
#include <iostream>
#include <cstdio>
#include <cassert>
#include <cmath>


class ByteBuffer
{
public:
    ByteBuffer(uint32_t size = BB_DEFAULT_SIZE);

    ByteBuffer(uint8_t *arr, uint32_t size, bool allocated_by_new = true);

    ByteBuffer(ByteBuffer &bb, uint32_t startId, uint32_t length);

    ByteBuffer(ByteBuffer &bb,uint32_t startId,uint32_t length,bool from_slice);

    ~ByteBuffer();

    std::shared_ptr<ByteBuffer> slice(uint32_t offset, uint32_t length);

    void filp();// reset the readPosition
    uint32_t bytesRemaining(); // Number of uint8_ts from the current read position till the end of the buffer
    void clear(); // Clear our the vector and reset read and write positions
    uint32_t size(); // Size of internal vector
    uint8_t *getPointer(); // get the pointer of bytebuffer
    void resetPosition();

    // Read
    uint8_t
    peek(); // Relative peek. Reads and returns the next uint8_t in the buffer from the current position but does not increment the read position
    uint8_t
    get(); // Relative get method. Reads the uint8_t at the buffers current position then increments the position
    uint8_t get(uint32_t index); // Absolute get method. Read uint8_t at index
    int getBufferOffset();
    uint8_t *getBuffer();
    // this is the same as read(byte b[], int off, int len) in InputStream.java
    int read(uint8_t *buffer, uint32_t off, uint32_t len);

    void getBytes(uint8_t *buffer, uint32_t len); // Absolute read into array buf of length len
    char getChar(); // Relative
    char getChar(uint32_t index); // Absolute
    double getDouble();

    double getDouble(uint32_t index);

    float getFloat();

    float getFloat(uint32_t index);

    int getInt();

    int getInt(uint32_t index);

    long getLong();

    long getLong(uint32_t index);

    short getShort();

    short getShort(uint32_t index);

    // Write
    void put(ByteBuffer *src); // Relative write of the entire contents of another ByteBuffer (src)
    void put(uint8_t b); // Relative write
    void put(uint8_t b, uint32_t index); // Absolute write at index
    void putBytes(uint8_t *b, uint32_t len); // Relative write
    void putBytes(uint8_t *b, uint32_t len, uint32_t index); // Absolute write starting at index
    void putChar(char value); // Relative
    void putChar(char value, uint32_t index); // Absolute
    void putDouble(double value);

    void putDouble(double value, uint32_t index);

    void putFloat(float value);

    void putFloat(float value, uint32_t index);

    void putInt(int value);

    void putInt(int value, uint32_t index);

    void putLong(long value);

    void putLong(long value, uint32_t index);

    void putShort(short value);

    void putShort(short value, uint32_t index);

    // Buffer Position Accessors & Mutators
    void setReadPos(uint32_t r)
    {
        rpos = r;
    }

    void skipBytes(uint32_t r)
    {
        rpos += r;
    }

    uint32_t getReadPos()
    {
        return rpos;
    }

    void setWritePos(uint32_t w)
    {
        wpos = w;
    }

    uint32_t getWritePos()
    {
        return wpos;
    }

    void markReaderIndex();

    void resetReaderIndex();

    // Utility Functions
    void setName(std::string n);

    std::string getName();

    void printInfo();

    void printAH();

    void printAscii();

    void printHex();

    void printPosition();

protected:
    uint32_t wpos;
    mutable uint32_t rpos;
    uint8_t *buf;
    uint32_t bufSize;
    std::string name;
    uint32_t rmark;
    bool fromOtherBB;
    bool fromSlice;
    // Sometimes the buffer is allocated by malloc/poxis_memalign, in this case, we
    // should use free() to deallocate the buf
    bool allocated_by_new;
private:
    template<typename T>
    T read()
    {
        T data = read<T>(rpos);
        rpos += sizeof(T);
        return data;
    }

    template<typename T>
    T read(uint32_t index)
    {
        if (index + sizeof(T) <= size())
        {
            T value;
            memcpy(&value, buf + index, sizeof(T));
            return value;
//			return *((T*) &buf[index]);
        }
        return 0;
    }

    template<typename T>
    void append(T data)
    {
        uint32_t s = sizeof(data);

        if (size() < (wpos + s))
        {
            throw std::runtime_error("Append exceeds the size of buffer");
        }
        memcpy(&buf[wpos], (uint8_t * ) & data, s);
        //printf("writing %c to %i\n", (uint8_t)data, wpos);

        wpos += s;
    }

    template<typename T>
    void insert(T data, uint32_t index)
    {
        if ((index + sizeof(data)) > size())
        {
            throw std::runtime_error("Insert exceeds the size of buffer");
        }

        memcpy(&buf[index], (uint8_t * ) & data, sizeof(data));

        wpos = index + sizeof(data);
    }
};


#endif