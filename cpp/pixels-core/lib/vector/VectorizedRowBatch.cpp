//
// Created by liyu on 3/7/23.
//

#include "vector/VectorizedRowBatch.h"

/*
 * This number is carefully chosen to minimize overhead and typically allows
 * one VectorizedRowBatch to fit in cache.
 */
int VectorizedRowBatch::DEFAULT_SIZE = 1024;

/**
 * Return a batch with the specified number of columns and rows.
 * Only call this constructor directly for testing purposes.
 * Batch size should normally always be defaultSize.
 *
 * @param numCols the number of columns to include in the batch
 * @param size    the number of rows to include in the batch
 */
VectorizedRowBatch::VectorizedRowBatch(int nCols, int size) {
    numCols = nCols;
    rowCount = 0;
    current = 0;
    maxSize = size;
    cols.clear();
    cols.resize(numCols);
	closed = false;
}

VectorizedRowBatch::~VectorizedRowBatch() {
	if(!closed) {
		close();
	}
}
/**
 * Returns the maximum size of the batch (number of rows it can hold)
 */
int VectorizedRowBatch::getMaxSize() {
    return maxSize;
}

/**
 * Return count of qualifying rows.
 *
 * @return number of rows that have not been filtered out
 */
int VectorizedRowBatch::count() {
    return rowCount;
}

/**
 * Whether this row batch is empty, i.e., contains no data.
 *
 * @return true if this row batch is empty
 */
bool VectorizedRowBatch::isEmpty() {
    return rowCount == 0;
}

/**
 * Whether this row batch is full, i.e., has no free space.
 *
 * @return true if this row batch is full
 */
bool VectorizedRowBatch::isFull() {
    return rowCount >= maxSize;
}

/**
 * @return the number of remaining slots in this row batch.
 */
int VectorizedRowBatch::freeSlots() {
    return maxSize - rowCount;
}

void VectorizedRowBatch::close() {
	if(!closed) {
		maxSize = 0;
		for(const auto& col : cols) {
			col->close();
		}
		cols.clear();
		closed = true;
	}
}

bool VectorizedRowBatch::isEndOfFile() {
    return closed || current >= rowCount;
}

uint64_t VectorizedRowBatch::position() {
    return current;
}

void VectorizedRowBatch::reset() {
    for(auto col: cols) {
        col->reset();
    }
    rowCount = 0;
    current = 0;
}

void VectorizedRowBatch::resize(int size) {
    for(auto col: cols) {
        col->resize(size);
    }
    maxSize = size;
}

void VectorizedRowBatch::increment(int size) {
    current += size;
    for (const auto& col: cols) {
        col->increment(size);
    }
}

uint64_t VectorizedRowBatch::remaining() {
    return rowCount - current;
}

