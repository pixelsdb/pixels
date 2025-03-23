//
// Created by liyu on 12/23/23.
//

#include <sstream>
#include <ctime>
#include <iomanip>

#include "vector/TimestampColumnVector.h"

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                    len * sizeof(long));
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}

void TimestampColumnVector::add(std::string& value) {
    std::tm tm = {};
    std::istringstream ss(value);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if(ss.fail()) {
        throw InvalidArgumentException("Invalid timestamp format");
    }
    long ts = (std::mktime(&tm)) * 1e6;
    add(ts);
}

void TimestampColumnVector::add(long value) {
    if(writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    times[index] = value;
    isNull[index] = false;
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        long *oldVector = times;
        posix_memalign(reinterpret_cast<void **>(&times), 32,
                        size * sizeof(int64_t));
        if (preserveData) {
            std::copy(oldVector, oldVector + length, times);
        }
        delete[] oldVector;
        memoryUsage += (long) sizeof(long) * (size - length);
        resize(size);
    }
}
