//
// Created by liyu on 3/17/23.
//

#include "vector/BinaryColumnVector.h"

BinaryColumnVector::BinaryColumnVector(int len, bool encoding): ColumnVector(len, encoding) {
    posix_memalign(reinterpret_cast<void **>(&vector), 4096,
                   len * sizeof(duckdb::string_t));
    memoryUsage += (long) sizeof(uint8_t) * len;
}

void BinaryColumnVector::close() {
	if(!closed) {
		ColumnVector::close();
		free(vector);
		vector = nullptr;

	}
}

void BinaryColumnVector::setRef(int elementNum, uint8_t * const &sourceBuf, int start, int length) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    this->vector[elementNum]
        = duckdb::string_t((char *)(sourceBuf + start), length);

    // TODO: isNull should implemented, but not now.

}

void BinaryColumnVector::print(int rowCount) {
	throw InvalidArgumentException("not support print binarycolumnvector.");
}

BinaryColumnVector::~BinaryColumnVector() {
	if(!closed) {
		BinaryColumnVector::close();
	}
}
