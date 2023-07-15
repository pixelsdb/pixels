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
    LongColumnVector(int len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false, bool isLong = true);
	~LongColumnVector();
    void print(int rowCount) override;
    void close() override;
};
#endif //PIXELS_LONGCOLUMNVECTOR_H
