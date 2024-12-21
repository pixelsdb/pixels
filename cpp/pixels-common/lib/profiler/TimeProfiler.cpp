/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

/*
 * @author liyu
 * @create 2023-05-03
 */
#include "profiler/TimeProfiler.h"



thread_local std::map<std::string,std::chrono::steady_clock::time_point> TimeProfiler::profiling;
thread_local std::map<std::string, long> TimeProfiler::localResult;

TimeProfiler &TimeProfiler::Instance() {
    static TimeProfiler instance;
    return instance;
}

TimeProfiler::TimeProfiler() {
}

void TimeProfiler::Start(const std::string& label) {
    if constexpr(enableProfile) {
        if (profiling.find(label) != profiling.end()) {
            throw InvalidArgumentException(
                    "TimeProfiler::Start: The same label has already been started. ");
        } else if (label.size() == 0) {
            throw InvalidArgumentException(
                    "TimeProfiler::Start: Label cannot be the empty string. ");
        } else {
            profiling[label] = std::chrono::steady_clock::now();
        }
    }
}

void TimeProfiler::End(const std::string& label) {
    if constexpr(enableProfile) {
        if (profiling.find(label) == profiling.end()) {
            throw InvalidArgumentException(
                    "TimeProfiler::End: The label is not started yet. ");
        } else if (label.size() == 0) {
            throw InvalidArgumentException(
                    "TimeProfiler::End: Label cannot be the empty string. ");
        }
        auto startTime = profiling[label];
        auto endTime = std::chrono::steady_clock::now();
		profiling.erase(label);
        if (localResult.find(label) == localResult.end()) {
            localResult[label] = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();
        } else {
            localResult[label] = localResult[label] + std::chrono::duration_cast<std::chrono::nanoseconds>
                    (endTime - startTime).count();
        }

    }
}

void TimeProfiler::Print() {
    if constexpr(enableProfile) {
        for(auto iter: globalResult) {
            std::cout<<iter.first<<" "<<1.0 * iter.second / 1000000000 <<"s(thread time)"<<std::endl;
        }
    }
}

void TimeProfiler::Reset() {
    profiling.clear();
    localResult.clear();
    globalResult.clear();
}

long TimeProfiler::Get(const std::string &label) {
    std::unique_lock<std::mutex> parallel_lock(lock);
    if(globalResult.find(label) != globalResult.end()) {
        return globalResult[label];
    } else {
        throw InvalidArgumentException(
                "TimeProfiler::Get: The label is not contained in Timeprofiler. ");
    }
}

int TimeProfiler::GetResultSize() {
    return globalResult.size();
}

void TimeProfiler::Collect() {
    std::unique_lock<std::mutex> parallel_lock(lock);
    for(auto iter: localResult) {
        auto label = iter.first;
        auto value = iter.second;
        if (globalResult.find(label) == globalResult.end()) {
            globalResult[label] = value;
        } else {
            globalResult[label] = globalResult[label] + value;
        }
    }
    localResult.clear();
}

