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
#ifndef PIXELS_COLUMNVECTOR_H
#define PIXELS_COLUMNVECTOR_H

/**
 * ColumnVector derived from org.apache.hadoop.hive.ql.exec.vector.
 * <p>
 * ColumnVector contains the shared structure for the sub-types,
 * including NULL information, and whether this vector
 * repeats, i.e. has all values the same, so only the first
 * one is set. This is used to accelerate query performance
 * by handling a whole vector in O(1) time when applicable.
 * <p>
 * The fields are public by design since this is a performance-critical
 * structure that is used in the inner loop of query execution.
 */

#include <iostream>
#include <memory>
#include "exception/InvalidArgumentException.h"

/**
 * ColumnVector derived from org.apache.hadoop.hive.ql.exec.vector.
 * <p>
 * ColumnVector contains the shared structure for the sub-types,
 * including NULL information, and whether this vector
 * repeats, i.e. has all values the same, so only the first
 * one is set. This is used to accelerate query performance
 * by handling a whole vector in O(1) time when applicable.
 * <p>
 * The fields are public by design since this is a performance-critical
 * structure that is used in the inner loop of query execution.
 */

class ColumnVector
{
public:
    /**
      * length is the capacity, i.e., maximum number of values, of this column vector
      * <b>DO NOT</b> modify it or used it as the number of values in-used.
      */
    uint64_t length;
    uint64_t writeIndex;
    uint64_t readIndex;
    uint64_t memoryUsage;
    bool closed;
    bool encoding;

    /**
     * If hasNulls is true, then this array contains true if the value
     * is null, otherwise false. The array is always allocated, so a batch can be re-used
     * later and nulls added.
     */
    uint8_t *isNull;

    // If the whole column vector has no nulls, this is true, otherwise false.
    bool noNulls;

    // DuckDB requires that the type of the valid mask should be uint64
    uint64_t *isValid;

    explicit ColumnVector(uint64_t len, bool encoding);

    void increment(uint64_t size);              // increment the readIndex
    bool isFull();                         // if the readIndex reaches length
    uint64_t position();                   // return readIndex
    void resize(int size);                 // resize the column vector to a smaller one
    virtual void close();

    virtual void reset();

    virtual void *current() = 0;              // get the pointer in the current location
    uint64_t *currentValid();

    virtual void print(int rowCount);      // this is only used for debug
    bool checkValid(int index);

    void addNull();

    virtual void ensureSize(uint64_t size, bool preserveData);

    virtual void add(std::string &value);

    virtual void add(bool value);

    virtual void add(int64_t value);

    virtual void add(int value);
};

#endif //PIXELS_COLUMNVECTOR_H
