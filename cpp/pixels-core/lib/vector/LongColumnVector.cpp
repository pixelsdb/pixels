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
#include "vector/LongColumnVector.h"
#include <algorithm>

LongColumnVector::LongColumnVector(uint64_t len, bool encoding, bool isLong)
        : ColumnVector (len, encoding)
{
    posix_memalign (reinterpret_cast<void **>(&longVector), 32,
                    len * sizeof (int64_t));

    memoryUsage += (long) sizeof (long) * len;
}

void LongColumnVector::close()
{
    if (!closed)
    {
        ColumnVector::close ();
        if (encoding && longVector != nullptr)
        {
            free (longVector);
        }
        longVector = nullptr;
    }
}

void LongColumnVector::print(int rowCount)
{
    throw InvalidArgumentException ("not support print longcolumnvector.");
    //    for(int i = 0; i < rowCount; i++) {
    //        std::cout<<longVector[i]<<std::endl;
    //		std::cout<<intVector[i]<<std::endl;
    //    }
}

LongColumnVector::~LongColumnVector()
{
    if (!closed)
    {
        LongColumnVector::close ();
    }
}

void *LongColumnVector::current()
{

    if (longVector == nullptr)
    {
        return nullptr;
    } else
    {
        return longVector + readIndex;
    }
}

void LongColumnVector::add(std::string &value)
{
    std::transform (value.begin (), value.end (), value.begin (), ::tolower);
    if (value == "true")
    {
        add (1);
    } else if (value == "false")
    {
        add (0);
    } else
    {
        add (std::stol (value));
    }
}

void LongColumnVector::add(bool value) { add (value ? 1 : 0); }

void LongColumnVector::add(int64_t value)
{
    if (writeIndex >= length)
    {
        ensureSize (writeIndex * 2, true);
    }
    int index = writeIndex++;

    longVector[index] = value;

    isNull[index] = false;
}

void LongColumnVector::add(int value)
{
    if (writeIndex >= length)
    {
        ensureSize (writeIndex * 2, true);
    }
    int index = writeIndex++;
    longVector[index] = value;

    isNull[index] = false;
}

void LongColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    ColumnVector::ensureSize (size, preserveData);
    if (length < size)
    {

        long *oldVector = longVector;
        posix_memalign (reinterpret_cast<void **>(&longVector), 32,
                        size * sizeof (int64_t));
        if (preserveData)
        {
            std::copy (oldVector, oldVector + length, longVector);
        }
        delete[] oldVector;
        memoryUsage += (long) sizeof (long) * (size - length);
        resize (size);
    }
}
