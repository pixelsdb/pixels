//
// Created by liyu on 7/6/23.
//

#ifndef DUCKDB_PIXELSFILTERMASK_H
#define DUCKDB_PIXELSFILTERMASK_H

#include <bitset>
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

#include "vector/ColumnVector.h"
#include "TypeDescription.h"

class pixelsFilterMask {
public:
    uint8_t * mask;
    long maskLength;
    long arrayLength;
    pixelsFilterMask(long length);
    pixelsFilterMask(pixelsFilterMask & other);
    ~pixelsFilterMask();
    void Or(pixelsFilterMask & other);
    void And(pixelsFilterMask & other);
    void Or(long index, uint8_t value);
    void And(long index, uint8_t value);
    bool isNone();
    void set();
    void set(long index, uint8_t value);
    void setByteAligned(long index, uint8_t value);
    uint8_t get(long index);
};

#endif //DUCKDB_PIXELSFILTERMASK_H
