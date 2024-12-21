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
#ifndef DUCKDB_TIMEPROFILER_H
#define DUCKDB_TIMEPROFILER_H

#include <iostream>
#include <memory>
#include <string>
#include "exception/InvalidArgumentException.h"
#include "profiler/AbstractProfiler.h"
#include <chrono>
#include <map>
#include <mutex>
#include <thread>

#define PROFILE_START(X) ::TimeProfiler::Instance().Start(X)
#define PROFILE_END(X) ::TimeProfiler::Instance().End(X)

class TimeProfiler : public AbstractProfiler
{
public:
    static TimeProfiler &Instance();

    void Start(const std::string &label);

    void End(const std::string &label);

    long Get(const std::string &label);

    void Reset() override;

    void Print() override;

    void Collect();

    int GetResultSize();

private:
    TimeProfiler();

    static thread_local std::map<std::string, std::chrono::steady_clock::time_point>
    profiling;
    static thread_local std::map<std::string, long>
    localResult;
    std::mutex lock;
    std::map<std::string, long> globalResult;
};

#endif //DUCKDB_TIMEPROFILER_H
