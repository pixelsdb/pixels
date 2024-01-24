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
