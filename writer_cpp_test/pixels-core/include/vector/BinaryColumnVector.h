//
// Created by liyu on 3/17/23.
//

#ifndef PIXELS_BINARYCOLUMNVECTOR_H
#define PIXELS_BINARYCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include <iostream>
#include <cstdlib>
#include <string>



/**
 * BinaryColumnVector derived from org.apache.hadoop.hive.ql.exec.vector.
 * <p>
 * This class supports string and binary data by value reference -- i.e. each field is
 * explicitly present, as opposed to provided by a dictionary reference.
 * In some cases, all the values will be in the same byte array to begin with,
 * but this need not be the case. If each value is in a separate byte
 * array to start with, or not all of the values are in the same original
 * byte array, you can still assign data by reference into this column vector.
 * This gives flexibility to use this in multiple situations.
 * <p>
 * When setting data by reference, the caller
 * is responsible for allocating the byte arrays used to hold the data.
 * You can also set data by value, as long as you call the initBuffer() method first.
 * You can mix "by value" and "by reference" in the same column vector,
 * though that use is probably not typical.
 */

class BinaryColumnVector: public ColumnVector {
public:
    std::string* vector;

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
    void setRef(int elementNum, uint8_t * const & sourceBuf, int start, int length);
    void * current() override;
    void close() override;
    void print(int rowCount) override;
};
#endif //PIXELS_BINARYCOLUMNVECTOR_H
