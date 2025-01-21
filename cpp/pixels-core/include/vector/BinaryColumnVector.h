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
#ifndef PIXELS_BINARYCOLUMNVECTOR_H
#define PIXELS_BINARYCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include "duckdb.h"
#include "duckdb/common/types/vector.hpp"

/**
 * BinaryColumnVector derived from org.apache.hadoop.hive.ql.exec.vector.
 * <p>
 * This class supports string and binary data by value reference -- i.e. each field is
 * explicitly present, as opposed to provided by a dictionary reference.
 * In some cases, all the values will be in the same byte array to begin with,
 * but this need not be the case. If each value is in a separate byte
 * array to start with, or not all the values are in the same original
 * byte array, you can still assign data by reference into this column vector.
 * This gives flexibility to use this in multiple situations.
 * <p>
 * When setting data by reference, the caller
 * is responsible for allocating the byte arrays used to hold the data.
 * You can also set data by value, as long as you call the initBuffer() method first.
 * You can mix "by value" and "by reference" in the same column vector,
 * though that use is probably not typical.
 */
class BinaryColumnVector : public ColumnVector
{
public:
    duckdb::string_t *vector;

    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    explicit BinaryColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false);

    ~BinaryColumnVector();

    /**
     * Set a field by reference.
     *
     * @param elementNum index within column vector to set
     * @param sourceBuf  container of source data
     * @param start      start byte position within source
     * @param length     length of source byte sequence
     */
    void setRef(int elementNum, uint8_t *const &sourceBuf, int start, int length);

    void add(std::string value);
    void add(uint8_t* v,int length);
    void setVal(int elemnetNum,uint8_t* sourceBuf);
    void setVal(int elementNum, uint8_t* sourceBuf, int start, int length);

    void *current() override;

    void close() override;

    void print(int rowCount) override;
};
#endif //PIXELS_BINARYCOLUMNVECTOR_H
