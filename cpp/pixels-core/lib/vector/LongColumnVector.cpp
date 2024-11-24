//
// Created by liyu on 3/17/23.
//

#include "vector/LongColumnVector.h"
#include <algorithm>

LongColumnVector::LongColumnVector(uint64_t len, bool encoding, bool isLong): ColumnVector(len, encoding) {
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

    this->isLong = isLong;
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

void * LongColumnVector::current() {
    if(isLong) {
        if(longVector == nullptr) {
            return nullptr;
        } else {
            return longVector + readIndex;
        }
    } else {
        if(intVector == nullptr) {
            return nullptr;
        } else {
            return intVector + readIndex;
        }
    }
}

void LongColumnVector::add(std::string &value) {
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value == "true") {
        add(1);
    } else if (value == "false") {
        add(0);
    } else {
        add(std::stol(value));
    }
}

void LongColumnVector::add(bool value) {
    add(value ? 1 : 0);
}

void LongColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    if (encoding) {
        int index = writeIndex++;
        if(isLong) {
            longVector[index] = value;
        } else {
            intVector[index] = value;
        }
        isNull[index] = false;
    }
}

void LongColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    if (encoding) {
        int index = writeIndex++;
        if(isLong) {
            longVector[index] = value;
        } else {
            intVector[index] = value;
        }
        isNull[index] = false;
    }
}


