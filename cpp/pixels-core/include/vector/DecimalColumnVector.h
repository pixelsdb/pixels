//
// Created by yuly on 05.04.23.
//

#ifndef PIXELS_DECIMALCOLUMNVECTOR_H
#define PIXELS_DECIMALCOLUMNVECTOR_H

#include "duckdb/common/types.hpp"
#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"

using PhysicalType = duckdb::PhysicalType;
class DecimalColumnVector : public ColumnVector {
  public:
    long *vector;
    int precision;
    int scale;
    PhysicalType physical_type_;
    static long DEFAULT_UNSCALED_VALUE;
    /**
    * Use this constructor by default. All column vectors
    * should normally be the default size.
    */
    DecimalColumnVector(int precision, int scale, bool encoding = false);
    DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding = false);
    ~DecimalColumnVector();
    void print(int rowCount) override;
    void close() override;
    void * current() override;
	int getPrecision();
	int getScale();

    void add(std::string &value) override;
    void add(long value) override;
    void ensureSize(uint64_t size, bool preserveData) override;
};

#endif // PIXELS_DECIMALCOLUMNVECTOR_H
