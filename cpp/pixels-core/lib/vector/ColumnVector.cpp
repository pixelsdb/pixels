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
 * @create 2023-03-07
 */
#include "vector/ColumnVector.h"
#include <cmath>

ColumnVector::ColumnVector(uint64_t len, bool encoding)
{
    writeIndex = 0;
    readIndex = 0;
    length = len;
    this->encoding = encoding;
    memoryUsage = len + sizeof(int) * 3 + 4;
    closed = false;
    isNull = new uint8_t[length]();
    noNulls = true;
    posix_memalign(reinterpret_cast<void **>(&isValid), 64, ceil(1.0 * len / 64) * sizeof(uint64_t));
}

ColumnVector::~ColumnVector()
{
}

void ColumnVector::close()
{
    if (!closed)
    {
        writeIndex = 0;
        closed = true;
        // TODO: reset other variables
        if (isValid != nullptr)
        {
            free(isValid);
            isValid = nullptr;
        }
        if (isNull != nullptr)
        {
            isNull = nullptr;
        }
    }
}

void ColumnVector::reset()
{
    writeIndex = 0;
    readIndex = 0;
    // TODO: reset other variables
}

void ColumnVector::print(int rowCount)
{
    throw InvalidArgumentException("This columnVector doesn't implement this function.");
}


idx_t ColumnVector::getCapacity() const{
    return length;
}

void ColumnVector::increment(uint64_t size)
{
    readIndex += size;
}

bool ColumnVector::isFull()
{
    return readIndex >= length;
}

uint64_t ColumnVector::position()
{
    return readIndex;
}

void ColumnVector::resize(int size)
{
    if (this->length < size)
    {
        throw InvalidArgumentException("column vector can only be resized to a smaller vector. ");
    }
    else
    {
        this->length = size;
    }
}

bool ColumnVector::checkValid(int index)
{
    int byteIndex = index / 8;
    int bitIndex = index % 8;
    auto *isValidByte = (uint8_t *) isValid;
    return isValidByte[byteIndex] & (1 << bitIndex);
}

uint64_t *ColumnVector::currentValid()
{
    return isValid + readIndex / 64;
}

void ColumnVector::addNull()
{
    if (writeIndex >= length)
    {
        ensureSize(writeIndex * 2, true);
    }
    this->isNull[writeIndex++] = true;
    this->noNulls = false;
}

void ColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    if (this->length < size)
    {
        uint8_t *oldArray = this->isNull;
        this->isNull = new uint8_t[size]();
        if (preserveData && !this->noNulls)
        {
            std::copy(oldArray, oldArray + this->length, this->isNull);
        }
        delete[] oldArray;
        resize(size);
    }
}

void ColumnVector::add(std::string &value)
{
    throw new std::runtime_error("Adding string is not supported");
}

void ColumnVector::add(bool value)
{
    throw new std::runtime_error("Adding boolean is not supported");
}

void ColumnVector::add(int64_t value)
{
    throw new std::runtime_error("Adding long is not supported");
}

void ColumnVector::add(int value)
{
    throw new std::runtime_error("Adding int is not supported");
}



