/**
 ByteBuffer
 ByteBuffer.cpp
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

#include <utility>

#include "physical/natives/ByteBuffer.h"



/**
 * ByteBuffer constructor
 * Reserves specified size in internal vector
 *
 * @param size Size (in bytes) of space to preallocate internally. Default is set in DEFAULT_SIZE
 */
ByteBuffer::ByteBuffer(uint32_t size) {
    buf = new uint8_t[size];
    bufSize = size;
    resetPosition();
    name = "";
    fromOtherBB = false;
	allocated_by_new = true;
    rmark = 0;
    wpos = 0;
    rpos = 0;
}


/**
 * ByteBuffer constructor
 * Consume an entire uint8_t array of length len in the ByteBuffer
 *
 * @param arr uint8_t array of data (should be of length len)
 * @param size Size of space to allocate
 */
ByteBuffer::ByteBuffer(uint8_t * arr, uint32_t size, bool allocated_by_new) {
    buf = arr;
    bufSize = size;
    resetPosition();
    name = "";
    fromOtherBB = false;
	this->allocated_by_new = allocated_by_new;

}

ByteBuffer::ByteBuffer(ByteBuffer & bb, uint32_t startId, uint32_t length) {
    assert(startId >= 0 && startId + length <= bb.size() && length > 0);
    buf = bb.getPointer() + startId;
    bufSize = length;
    resetPosition();
    name = "";
    fromOtherBB = true;
	allocated_by_new = true;
}

/**
 * Buffer flip
 * set the readPosition to the beginning
 */
void ByteBuffer::filp() {
    if(size()>0){
        rpos=0;
    }
}
/**
 * Bytes Remaining
 * Returns the number of bytes from the current read position till the end of the buffer
 *
 * @return Number of bytes from rpos to the end (size())
 */
uint32_t ByteBuffer::bytesRemaining() {
    return size() - rpos;
}


void ByteBuffer::clear() {
    resetPosition();
	if(!fromOtherBB) {
		if(buf != nullptr) {
			if(allocated_by_new) {
				delete[] buf;
			} else {
				free(buf);
			}
		}
	}
	buf = nullptr;
    bufSize = 0;
}

void ByteBuffer::resetPosition() {
    rpos = 0;
    wpos = 0;
    rmark = 0;
}
/**
 * Size
 * Returns the size of the internal buffer...not necessarily the length of bytes used as data!
 *
 * @return size of the internal buffer
 */
uint32_t ByteBuffer::size() {
    return bufSize;
}

// Replacement


// Read Functions

uint8_t ByteBuffer::peek() {
    return read<uint8_t>(rpos);
}

uint8_t ByteBuffer::get() {
    return read<uint8_t>();
}

uint8_t ByteBuffer::get(uint32_t index) {
    return read<uint8_t>(index);
}

void ByteBuffer::getBytes(uint8_t* buffer, uint32_t len) {
    for (uint32_t i = 0; i < len; i++) {
        buffer[i] = read<uint8_t>();
    }
}

char ByteBuffer::getChar() {
    return read<char>();
}

char ByteBuffer::getChar(uint32_t index) {
    return read<char>(index);
}

double ByteBuffer::getDouble() {
    return read<double>();
}

double ByteBuffer::getDouble(uint32_t index) {
    return read<double>(index);
}

float ByteBuffer::getFloat() {
    return read<float>();
}

float ByteBuffer::getFloat(uint32_t index) {
    return read<float>(index);
}

int ByteBuffer::getInt() {
    return (int) read<int>();
}

int ByteBuffer::getInt(uint32_t index) {
    return (int) read<int>(index);
}

long ByteBuffer::getLong() {
    //TODO: if other type should use this function?
    return (long)read<uint64_t>();
}

long ByteBuffer::getLong(uint32_t index) {
    return (long)read<uint64_t>(index);
}

short ByteBuffer::getShort() {
    return read<short>();
}

short ByteBuffer::getShort(uint32_t index) {
    return read<short>(index);
}

int ByteBuffer::read(uint8_t * buffer, uint32_t off, uint32_t len) {
    int actualLen = std::min((int)len, (int)bytesRemaining());
    if(actualLen == 0) {
        return 0;
    }
    memcpy(buffer + off, buf + rpos, actualLen);
    rpos += actualLen;
    return actualLen;
}

// Write Functions

void ByteBuffer::put(ByteBuffer* src) {
    uint32_t len = src->size();
    for (uint32_t i = 0; i < len; i++)
        append<uint8_t>(src->get(i));
}

void ByteBuffer::put(uint8_t b) {
    append<uint8_t>(b);
}

void ByteBuffer::put(uint8_t b, uint32_t index) {
    insert<uint8_t>(b, index);
}

void ByteBuffer::putBytes(uint8_t* b, uint32_t len) {
    // Insert the data one byte at a time into the internal buffer at position i+starting index
    for (uint32_t i = 0; i < len; i++)
        append<uint8_t>(b[i]);
}

void ByteBuffer::putBytes(uint8_t* b, uint32_t len, uint32_t index) {
    wpos = index;

    // Insert the data one byte at a time into the internal buffer at position i+starting index
    for (uint32_t i = 0; i < len; i++)
        append<uint8_t>(b[i]);
}

void ByteBuffer::putChar(char value) {
    append<char>(value);
}

void ByteBuffer::putChar(char value, uint32_t index) {
    insert<char>(value, index);
}

void ByteBuffer::putDouble(double value) {
    append<double>(value);
}

void ByteBuffer::putDouble(double value, uint32_t index) {
    insert<double>(value, index);
}
void ByteBuffer::putFloat(float value) {
    append<float>(value);
}

void ByteBuffer::putFloat(float value, uint32_t index) {
    insert<float>(value, index);
}

void ByteBuffer::putInt(int value) {
    append<int>(value);
}

void ByteBuffer::putInt(int value, uint32_t index) {
    insert<int>(value, index);
}

void ByteBuffer::putLong(long value) {
    append<long>(value);
}

void ByteBuffer::putLong(long value, uint32_t index) {
    insert<long>(value, index);
}

void ByteBuffer::putShort(short value) {
    append<short>(value);
}

void ByteBuffer::putShort(short value, uint32_t index) {
    insert<short>(value, index);
}

// Utility Functions
void ByteBuffer::setName(std::string n) {
	name = std::move(n);
}

std::string ByteBuffer::getName() {
	return name;
}

void ByteBuffer::printInfo() {
	uint32_t length = size();
	std::cout << "ByteBuffer " << name.c_str() << " Length: " << length << ". Info Print" << std::endl;
}

void ByteBuffer::printAH() {
	uint32_t length = size();
	std::cout << "ByteBuffer " << name.c_str() << " Length: " << length << ". ASCII & Hex Print" << std::endl;

	for (uint32_t i = 0; i < length; i++) {
		std::printf("0x%02x ", buf[i]);
	}

	std::printf("\n");
	for (uint32_t i = 0; i < length; i++) {
		std::printf("%c ", buf[i]);
	}

	std::printf("\n");
}

void ByteBuffer::printAscii() {
	uint32_t length = size();
	std::cout << "ByteBuffer " << name.c_str() << " Length: " << length << ". ASCII Print" << std::endl;

	for (uint32_t i = 0; i < length; i++) {
		std::printf("%c ", buf[i]);
	}

	std::printf("\n");
}

void ByteBuffer::printHex() {
	uint32_t length = size();
	std::cout << "ByteBuffer " << name.c_str() << " Length: " << length << ". Hex Print" << std::endl;

	for (uint32_t i = 0; i < length; i++) {
		std::printf("0x%02x ", buf[i]);
	}

	std::printf("\n");
}

void ByteBuffer::printPosition() {
	uint32_t length = size();
	std::cout << "ByteBuffer " << name.c_str() << " Length: " << length << " Read Pos: " << rpos << ". Write Pos: "
	        << wpos << std::endl;
}

ByteBuffer::~ByteBuffer() {
    if(!fromOtherBB) {
		if(buf != nullptr) {
			if(allocated_by_new) {
				delete[] buf;
			} else {
				free(buf);
			}
		}
    }
    buf = nullptr;
}

uint8_t *ByteBuffer::getPointer() {
    return buf;
}

void ByteBuffer::markReaderIndex() {
    rmark = rpos;
}

// this is different from resetPosition. This function should be called after markReaderIndex.
// Then the rpos is reset to rmark.
void ByteBuffer::resetReaderIndex() {
    rpos = rmark;
}



/**
 *
 * @return internal buffers
 */
uint8_t *ByteBuffer::getBuffer() {
    return buf+rpos;
}

/**
 *
 * @return internal buffers first element's offset
 */
int ByteBuffer::getBufferOffset() {
    return rpos;
//    return 0;
}



