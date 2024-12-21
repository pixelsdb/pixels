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
#ifndef DUCKDB_COUNTPROFILER_H
#define DUCKDB_COUNTPROFILER_H

#include <iostream>
#include <memory>
#include <string>
#include "exception/InvalidArgumentException.h"
#include "profiler/AbstractProfiler.h"
#include <chrono>
#include <map>
#include <mutex>


// This class is used for showing how many times a function is invoked.

class CountProfiler : public AbstractProfiler
{
public:
    static CountProfiler &Instance();

    void Count(const std::string &label);

    void Count(const std::string &label, int num);

    void Print() override;

    void Reset() override;

    long Get(const std::string &label);

private:
    std::mutex lock;
    std::map<std::string, long> result;
};


#endif //DUCKDB_COUNTPROFILER_H
