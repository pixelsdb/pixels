//
// Created by yuly on 03.05.23.
//

#include "profiler/CountProfiler.h"

CountProfiler &CountProfiler::Instance() {
    static CountProfiler instance;
    return instance;
}

void CountProfiler::Count(const std::string &label) {
    if constexpr(enableProfile) {
        std::unique_lock<std::mutex> parallel_lock(lock);
        if (result.find(label) != result.end()) {
            result[label] += 1;
        } else if (label.size() == 0) {
            throw InvalidArgumentException(
                    "TimeProfiler::Start: Label cannot be the empty string. ");
        } else {
            result[label] = 1;
        }
    }
}

void CountProfiler::Count(const std::string &label, int num) {
	if constexpr(enableProfile) {
		std::unique_lock<std::mutex> parallel_lock(lock);
		if (result.find(label) != result.end()) {
			result[label] += num;
		} else if (label.size() == 0) {
			throw InvalidArgumentException(
			    "TimeProfiler::Start: Label cannot be the empty string. ");
		} else {
			result[label] = num;
		}
	}
}

void CountProfiler::Print() {
    if constexpr(enableProfile) {
        for(auto iter: result) {
            std::cout<< "The count of " <<iter.first<<" is "<<iter.second<<std::endl;
        }
    }
}

void CountProfiler::Reset() {
    result.clear();
}

long CountProfiler::Get(const std::string &label) {
    std::unique_lock<std::mutex> parallel_lock(lock);
    if(result.find(label) != result.end()) {
        return result[label];
    } else {
        throw InvalidArgumentException(
                "CountProfiler::Get: The label is not contained in CountProfiler. ");
    }
}

