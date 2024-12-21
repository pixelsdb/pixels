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
#include "vector/ByteColumnVector.h"

ByteColumnVector::ByteColumnVector(int len, bool encoding) : ColumnVector(len, encoding)
{
    vector = new uint8_t[len];
    memoryUsage += (long) sizeof(uint8_t) * len;
}

void ByteColumnVector::close()
{
    if (!closed)
    {
        ColumnVector::close();
        delete[] vector;
        vector = nullptr;
    }
}