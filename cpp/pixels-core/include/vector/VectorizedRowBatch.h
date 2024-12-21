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
#ifndef PIXELS_VECTORIZEDROWBATCH_H
#define PIXELS_VECTORIZEDROWBATCH_H

/**
 * VectorizedRowBatch derived from org.apache.hadoop.hive.ql.exec.vector
 * <p>
 * A VectorizedRowBatch is a set of rows, organized with each column
 * as a vector. It is the unit of query execution, organized to minimize
 * the cost per row and achieve high cycles-per-instruction.
 * The major fields are public by design to allow fast and convenient
 * access by the vectorized query execution code.
 */

#include <iostream>
#include <vector>
#include "vector/ColumnVector.h"
#include <memory>

class VectorizedRowBatch
{
public:
    int numCols;                                       // number of columns
    std::vector <std::shared_ptr<ColumnVector>> cols;   // a vector for each column
    int rowCount;                                       // number of rows that qualify, i.e., haven't been filtered out
    static int DEFAULT_SIZE;
    int maxSize;                                       // capacity, i.e. the maximum number of rows can be stored in this row batch

    explicit VectorizedRowBatch(int nCols, int size = DEFAULT_SIZE);

    ~VectorizedRowBatch();

    void close();

    int getMaxSize();

    void reset();

    void resize(int size);

    uint64_t position();

    uint64_t remaining();

    void increment(int size);

    int count();

    bool isEmpty();

    bool isFull();

    int freeSlots();

    bool isEndOfFile();

private:
    bool closed;
    int current;                                        // The current pointer of VectorizedRowBatch.
};
#endif //PIXELS_VECTORIZEDROWBATCH_H
