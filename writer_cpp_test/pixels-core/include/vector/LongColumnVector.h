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
	long * intVector;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    explicit LongColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false, bool isLong = true);
    void * current() override;
	~LongColumnVector();
    void print(int rowCount) override;
    void close() override;
    void add(std::string &value) override;
    void add(bool value) override;
    void add(int64_t value) override;
    void add(int value) override;
    void ensureSize(uint64_t size, bool preserveData) override;
    bool isLongVectore();
private:
    bool isLong;
};
#endif //PIXELS_LONGCOLUMNVECTOR_H
