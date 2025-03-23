//
// Created by yuly on 06.04.23.
//

#include <sstream>
#include <ctime>
#include <iomanip>

#include "vector/DateColumnVector.h"

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
    posix_memalign(reinterpret_cast<void **>(&dates), 32,
                    len * sizeof(int32_t));
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::add(std::string& value) {
	std::tm tm = {};
    std::istringstream ss(value);

    ss >> std::get_time(&tm, "%Y-%m-%d");
    if (ss.fail()) {
        std::cerr << "Error parsing date." << std::endl;
        return;
    }

    std::time_t time_since_epoch = std::mktime(&tm);
    if (time_since_epoch == -1) {
        std::cerr << "Error converting to time_t." << std::endl;
        return;
    }

    const int SECONDS_IN_A_DAY = 86400;
    int days_since_epoch = time_since_epoch / SECONDS_IN_A_DAY + 1;

    add(days_since_epoch);

	return;
}

void DateColumnVector::add(int value) {
	if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex++;
    dates[index] = value;
    isNull[index] = false;
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (length < size) {
        int *oldVector = dates;
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                        size * sizeof(int32_t));
        if (preserveData) {
            std::copy(oldVector, oldVector + length, dates);
        }
        delete[] oldVector;
        memoryUsage += (long) sizeof(long) * (size - length);
        resize(size);
    }
}
