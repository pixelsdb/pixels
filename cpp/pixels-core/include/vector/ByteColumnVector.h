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
#ifndef PIXELS_BYTECOLUMNVECTOR_H
#define PIXELS_BYTECOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class ByteColumnVector : public ColumnVector
{
public:
    uint8_t *vector;

    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    ByteColumnVector(int len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false);

    void close() override;
};
#endif //PIXELS_BYTECOLUMNVECTOR_H
