//
// Created by liyu on 3/7/23.
//

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

class VectorizedRowBatch {
public:
    int numCols;                                       // number of columns
    std::vector<std::shared_ptr<ColumnVector>> cols;   // a vector for each column
    int rowCount;                                       // number of rows that qualify, i.e., haven't been filtered out
    static int DEFAULT_SIZE;
    int maxSize;                                       // capacity, i.e. the maximum number of rows can be stored in this row batch

    // If this is true, then there is no data in the batch -- we have hit the end of input
    bool endOfFile;
    explicit VectorizedRowBatch(int nCols, int size = DEFAULT_SIZE);
	~VectorizedRowBatch();
	void close();
    int getMaxSize();
    int count();
    bool isEmpty();
    bool isFull();
    int freeSlots();

private:
	bool closed;
};
#endif //PIXELS_VECTORIZEDROWBATCH_H
