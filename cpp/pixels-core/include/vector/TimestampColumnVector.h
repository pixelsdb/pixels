//
// Created by liyu on 12/23/23.
//

#ifndef DUCKDB_TIMESTAMPCOLUMNVECTOR_H
#define DUCKDB_TIMESTAMPCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

class TimestampColumnVector: public ColumnVector {
public:
    int precision;
    long * times;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    explicit TimestampColumnVector(int precision, bool encoding = false);
    explicit TimestampColumnVector(uint64_t len, int precision, bool encoding = false);
    void * current() override;
    void set(int elementNum, long ts);
    ~TimestampColumnVector();
    void print(int rowCount) override;
    void close() override;

    void add(std::string &value) override;
    void add(long value) override;
    void ensureSize(uint64_t size, bool preserveData) override;
private:
    bool isLong;
};
#endif //DUCKDB_TIMESTAMPCOLUMNVECTOR_H
