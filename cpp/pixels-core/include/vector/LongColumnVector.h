//
// Created by liyu on 3/17/23.
//

#ifndef PIXELS_LONGCOLUMNVECTOR_H
#define PIXELS_LONGCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class LongColumnVector: public ColumnVector {
public:
    long * longVector;
	int * intVector;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    explicit LongColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false, bool isLong = true);
    void * current() override;
	~LongColumnVector();
    void print(int rowCount) override;
    void close() override;
private:
    bool isLong;
};
#endif //PIXELS_LONGCOLUMNVECTOR_H
