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
 * @create 2023-03-17
 */
#include "vector/BinaryColumnVector.h"
#include <cstdlib> // 用于 posix_memalign 和 free
#include <cstring> // 用于 memcpy

BinaryColumnVector::BinaryColumnVector(uint64_t len, bool encoding)
    : ColumnVector(len, encoding)
{
    str_vec.resize(len);
    memoryUsage += sizeof(std::string) * len;
}

void BinaryColumnVector::close()
{
    if (!closed)
    {
        ColumnVector::close();
        str_vec.clear();
        str_vec.shrink_to_fit();
        closed = true;
    }
}

void BinaryColumnVector::setRef(int elementNum,
                                uint8_t *const &sourceBuf,
                                int start,
                                int length)
{
    if (elementNum >= (int)this->length)
    {
        ensureSize(elementNum + 1, true);
    }

    if (elementNum >= writeIndex)
    {
        writeIndex = elementNum + 1;
    }

    str_vec[elementNum] = std::string(
        reinterpret_cast<char *>(sourceBuf + start),
        length);

    isNull[elementNum] = false;
}

void BinaryColumnVector::setVal(int elementNum,
                                uint8_t *sourceBuf,
                                int start,
                                int length)
{
    str_vec[elementNum] = std::string(
        reinterpret_cast<char *>(sourceBuf + start),
        length);

    isNull[elementNum] = false;
}

void BinaryColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    if (length >= size)
        return;

    if (preserveData)
    {
        str_vec.resize(size);
    }
    else
    {
        std::vector<std::string> new_vec(size);
        str_vec.swap(new_vec);
    }

    memoryUsage += sizeof(std::string) * (size - length);

    resize(size);  // 更新基类 length
}


BinaryColumnVector::~BinaryColumnVector()
{
    if (!closed)
    {
        BinaryColumnVector::close();
    }
}

void *BinaryColumnVector::current()
{
    if (readIndex >= str_vec.size())
        return nullptr;

    return (void *)&str_vec[readIndex];
}

void BinaryColumnVector::add(std::string &value)
{
    if (writeIndex >= (int)length)
    {
        ensureSize(writeIndex == 0 ? 1 : writeIndex * 2, true);
    }

    str_vec[writeIndex++] = value;
}

void BinaryColumnVector::add(uint8_t *v, int len)
{
    if (writeIndex >= (int)length)
    {
        ensureSize(writeIndex == 0 ? 1 : writeIndex * 2, true);
    }

    str_vec[writeIndex++] =
        std::string(reinterpret_cast<char *>(v), len);
}

const std::string &BinaryColumnVector::getValue(idx_t i) const
{
    return str_vec[i];
}

bool  BinaryColumnVector::isNullAt(idx_t i) const
{
    return isNull[i];
}