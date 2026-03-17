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
 * @create 2023-04-05
 */
#include <algorithm>
#include <cstring>
#include <cmath>
#include <iostream>
#include <cstdlib>
#include "vector/DecimalColumnVector.h"

DecimalColumnVector::DecimalColumnVector(
        uint64_t len,
        int precision,
        int scale,
        bool encoding)
    : ColumnVector(len, encoding),
      precision_(precision),
      scale_(scale)
{
    if (precision <= pixels::DecimalConfig::MAX_WIDTH_INT16)
        physical_type_ = pixels::PhysicalType::INT16;
    else if (precision <= pixels::DecimalConfig::MAX_WIDTH_INT32)
        physical_type_ = pixels::PhysicalType::INT32;
    else if (precision <= pixels::DecimalConfig::MAX_WIDTH_INT64)
        physical_type_ = pixels::PhysicalType::INT64;
    else if (precision <= pixels::DecimalConfig::MAX_WIDTH_INT128)
        physical_type_ = pixels::PhysicalType::INT128;
    else
        throw std::runtime_error("Decimal precision too large");

    size_t bytes = len * ElementSize();

    if (posix_memalign(&vector, 32, bytes) != 0)
        throw std::runtime_error("Decimal allocation failed");

    memoryUsage += bytes;
}

void DecimalColumnVector::close()
{
    if (!closed)
    {
        ColumnVector::close();

        if (physical_type_ == pixels::PhysicalType::INT16 ||
            physical_type_ == pixels::PhysicalType::INT32)
        {
            free (vector);
        }

        closed = true;
    }
}


DecimalColumnVector::~DecimalColumnVector()
{
    if (!closed)
    {
        DecimalColumnVector::close();
    }
}

void* DecimalColumnVector::current()
{
    if (!vector) return nullptr;

    return static_cast<char*>(vector) +
           readIndex * ElementSize();
}

void DecimalColumnVector::add(const std::string& input)
{
    std::string value = input;

    auto dot_pos = value.find('.');
    int fractional_digits = 0;

    if (dot_pos != std::string::npos)
        fractional_digits = value.size() - dot_pos - 1;

    std::string merged =
        dot_pos == std::string::npos ?
        value :
        value.substr(0, dot_pos) + value.substr(dot_pos + 1);

    __int128 integerValue = 0;

    for (char c : merged)
    {
        if (c == '-') continue;
        integerValue = integerValue * 10 + (c - '0');
    }

    if (value[0] == '-')
        integerValue = -integerValue;

    int scale_diff = scale_ - fractional_digits;

    for (int i = 0; i < scale_diff; i++)
        integerValue *= 10;

    add((long)integerValue);  // 自动走分支
}

void DecimalColumnVector::add(long value)
{
    if (writeIndex >= length)
        ensureSize(length * 2, true);

    switch (physical_type_)
    {
        case pixels::PhysicalType::INT16:
            reinterpret_cast<int16_t*>(vector)[writeIndex] = (int16_t)value;
            break;

        case pixels::PhysicalType::INT32:
            reinterpret_cast<int32_t*>(vector)[writeIndex] = (int32_t)value;
            break;

        case pixels::PhysicalType::INT64:
            reinterpret_cast<int64_t*>(vector)[writeIndex] = (int64_t)value;
            break;

        case pixels::PhysicalType::INT128:
            reinterpret_cast<__int128*>(vector)[writeIndex] = (__int128)value;
            break;
    }

    isNull[writeIndex] = false;
    writeIndex++;
}

size_t DecimalColumnVector::ElementSize() const
{
    switch (physical_type_)
    {
        case pixels::PhysicalType::INT16:  return sizeof(int16_t);
        case pixels::PhysicalType::INT32:  return sizeof(int32_t);
        case pixels::PhysicalType::INT64:  return sizeof(int64_t);
        case pixels::PhysicalType::INT128: return sizeof(__int128);
        default:
            throw std::runtime_error("Invalid decimal physical type");
    }
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    if (size <= length)
        return;

    void* old = vector;
    size_t old_bytes = length * ElementSize();
    size_t new_bytes = size * ElementSize();

    if (posix_memalign(&vector, 32, new_bytes) != 0)
        throw std::runtime_error("Decimal resize failed");

    if (preserveData && old)
        std::memcpy(vector, old, old_bytes);

    free(old);

    memoryUsage += (new_bytes - old_bytes);
    resize(size);
}
