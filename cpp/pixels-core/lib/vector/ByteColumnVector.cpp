//
// Created by liyu on 3/17/23.
//

#include "vector/ByteColumnVector.h"

ByteColumnVector::ByteColumnVector(int len, bool encoding): ColumnVector(len, encoding) {
    vector = new uint8_t[len];
    memoryUsage += (long) sizeof(uint8_t) * len;
}

void ByteColumnVector::close() {
	if(!closed) {
		ColumnVector::close();
		delete[] vector;
		vector = nullptr;
	}
}