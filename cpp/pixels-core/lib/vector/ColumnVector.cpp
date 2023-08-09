//
// Created by liyu on 3/7/23.
//

#include "vector/ColumnVector.h"

ColumnVector::ColumnVector(uint64_t len, bool encoding) {
    writeIndex = 0;
    readIndex = 0;
    length = len;
	this->encoding = encoding;
    memoryUsage = len + sizeof(int) * 3 + 4;
	closed = false;
}

void ColumnVector::close() {
	if(!closed) {
		writeIndex = 0;
		closed = true;
		// TODO: reset other variables
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





