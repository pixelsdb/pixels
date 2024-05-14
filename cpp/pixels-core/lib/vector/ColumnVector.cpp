//
// Created by liyu on 3/7/23.
//

#include "vector/ColumnVector.h"
#include <cmath>
ColumnVector::ColumnVector(uint64_t len, bool encoding) {
    writeIndex = 0;
    readIndex = 0;
    length = len;
	this->encoding = encoding;
    memoryUsage = len + sizeof(int) * 3 + 4;
	closed = false;
    isNull = nullptr;
    posix_memalign(reinterpret_cast<void **>(&isValid), 64, ceil(1.0 * len / 64) * sizeof(uint64_t));
}

void ColumnVector::close() {
	if(!closed) {
        writeIndex = 0;
        closed = true;
        // TODO: reset other variables
        if (isValid != nullptr) {
            free(isValid);
            isValid = nullptr;
        }
    }
}

void ColumnVector::reset() {
    writeIndex = 0;
    readIndex = 0;
    // TODO: reset other variables
}

void ColumnVector::print(int rowCount) {
    throw InvalidArgumentException("This columnVector doesn't implement this function.");
}

void ColumnVector::increment(uint64_t size) {
    readIndex += size;
}

bool ColumnVector::isFull() {
    return readIndex >= length;
}

uint64_t ColumnVector::position() {
    return readIndex;
}

void ColumnVector::resize(int size) {
    if(this->length < size) {
        throw InvalidArgumentException("column vector can only be resized to a smaller vector. ");
    } else {
        this->length = size;
    }
}

bool ColumnVector::checkValid(int index) {
    int byteIndex = index / 8;
    int bitIndex = index % 8;
    auto * isValidByte = (uint8_t *)isValid;
    return isValidByte[byteIndex] & (1 << bitIndex);
}

uint64_t * ColumnVector::currentValid() {
    return isValid + readIndex / 64;
}





