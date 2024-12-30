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
#include "profiler/CountProfiler.h"

CountProfiler &CountProfiler::Instance()
{
    static CountProfiler instance;
    return instance;
}

void CountProfiler::Count(const std::string &label)
{
    if constexpr(enableProfile)
    {
        std::unique_lock <std::mutex> parallel_lock(lock);
        if (result.find(label) != result.end())
        {
            result[label] += 1;
        }
        else if (label.size() == 0)
        {
            throw InvalidArgumentException(
                    "TimeProfiler::Start: Label cannot be the empty string. ");
        }
        else
        {
            result[label] = 1;
        }
    }
}

void CountProfiler::Count(const std::string &label, int num)
{
    if constexpr(enableProfile)
    {
        std::unique_lock <std::mutex> parallel_lock(lock);
        if (result.find(label) != result.end())
        {
            result[label] += num;
        }
        else if (label.size() == 0)
        {
            throw InvalidArgumentException(
                    "TimeProfiler::Start: Label cannot be the empty string. ");
        }
        else
        {
            result[label] = num;
        }
    }
}

void CountProfiler::Print()
{
    if constexpr(enableProfile)
    {
        for (auto iter: result)
        {
            std::cout << "The count of " << iter.first << " is " << iter.second << std::endl;
        }
    }
}

void CountProfiler::Reset()
{
    result.clear();
}

long CountProfiler::Get(const std::string &label)
{
    std::unique_lock <std::mutex> parallel_lock(lock);
    if (result.find(label) != result.end())
    {
        return result[label];
    }
    else
    {
        throw InvalidArgumentException(
                "CountProfiler::Get: The label is not contained in CountProfiler. ");
    }
}

