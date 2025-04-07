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
#ifndef PIXELS_LONGCOLUMNVECTOR_H
#define PIXELS_LONGCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class LongColumnVector : public ColumnVector
{
public:
    long *longVector;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    explicit LongColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false,
                              bool isLong = true);

    void *current() override;

    ~LongColumnVector();

    void print(int rowCount) override;

    void close() override;

    void add(std::string &value) override;

    void add(bool value) override;

    void add(int64_t value) override;

    void add(int value) override;

    void ensureSize(uint64_t size, bool preserveData) override;

};
#endif //PIXELS_LONGCOLUMNVECTOR_H
