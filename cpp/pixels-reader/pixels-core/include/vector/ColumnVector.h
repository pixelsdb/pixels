//
// Created by liyu on 3/7/23.
//

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

class ColumnVector {
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
    explicit ColumnVector(uint64_t len, bool encoding);
    void increment(uint64_t size);              // increment the readIndex
    bool isFull();                         // if the readIndex reaches length
    uint64_t position();                   // return readIndex
    virtual void close();
    virtual void reset();
    virtual void * current() = 0;              // get the pointer in the current location
    virtual void print(int rowCount);      // this is only used for debug
};

#endif //PIXELS_COLUMNVECTOR_H
