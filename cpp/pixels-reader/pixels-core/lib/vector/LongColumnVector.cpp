//
// Created by liyu on 3/17/23.
//

#include "vector/LongColumnVector.h"

LongColumnVector::LongColumnVector(int len, bool encoding, bool isLong): ColumnVector(len, encoding) {
	if(encoding) {
		if(isLong) {
			longVector = new int64_t[len];
			intVector = nullptr;
		} else {
			longVector = nullptr;
			intVector = new int[len];
		}
	} else {
		longVector = nullptr;
		intVector = nullptr;
	}

    memoryUsage += (long) sizeof(long) * len;
}

void LongColumnVector::close() {
	if(!closed) {
		ColumnVector::close();
		if(encoding && longVector != nullptr) {
			delete[] longVector;
		}
		if(encoding && intVector != nullptr) {
			delete[] intVector;
		}
		longVector = nullptr;
		intVector = nullptr;
	}
}

void LongColumnVector::print(int rowCount) {
	throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

LongColumnVector::~LongColumnVector() {
	if(!closed) {
		LongColumnVector::close();
	}
}
