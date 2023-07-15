//
// Created by liyu on 3/17/23.
//

#include "vector/BinaryColumnVector.h"

BinaryColumnVector::BinaryColumnVector(int len, bool encoding): ColumnVector(len, encoding) {
    vector = new uint8_t *[len];
    start = new int[len];
    lens = new int[len];
    memoryUsage += (long) sizeof(uint8_t) * len;
}

void BinaryColumnVector::close() {
	if(!closed) {
		ColumnVector::close();
		delete[] vector;
		vector = nullptr;
		delete[] start;
		start = nullptr;
		delete[] lens;
		lens = nullptr;
	}
}

void BinaryColumnVector::setRef(int elementNum, uint8_t * const &sourceBuf, int start, int length) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    this->vector[elementNum] = sourceBuf;
    this->start[elementNum] = start;
    this->lens[elementNum] = length;
    // TODO: isNull should implemented, but not now.

}

void BinaryColumnVector::print(int rowCount) {
//	throw InvalidArgumentException("not support print binarycolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        int s = start[i];
        int l = lens[i];
        for(int j = 0; j < l; j++) {
            std::cout<<(char)(*(vector[i] + s + j));
        }
        std::cout<<std::endl;
    }
}
BinaryColumnVector::~BinaryColumnVector() {
	if(!closed) {
		BinaryColumnVector::close();
	}
}
