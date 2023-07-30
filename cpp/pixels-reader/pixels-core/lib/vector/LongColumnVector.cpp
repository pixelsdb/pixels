//
// Created by liyu on 3/17/23.
//

#include "vector/LongColumnVector.h"

LongColumnVector::LongColumnVector(int len, bool encoding, bool isLong): ColumnVector(len, encoding) {
	if(encoding) {
		if(isLong) {
            posix_memalign(reinterpret_cast<void **>(&longVector), 32,
                           len * sizeof(int64_t));
			intVector = nullptr;
		} else {
			longVector = nullptr;
            posix_memalign(reinterpret_cast<void **>(&intVector), 32,
                           len * sizeof(int32_t));
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
			free(longVector);
		}
		if(encoding && intVector != nullptr) {
			free(intVector);
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
