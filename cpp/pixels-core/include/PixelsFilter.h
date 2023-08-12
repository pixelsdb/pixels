//
// Created by liyu on 6/23/23.
//

#ifndef DUCKDB_PIXELSFILTER_H
#define DUCKDB_PIXELSFILTER_H

#include <bitset>
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "PixelsBitMask.h"
#include "vector/ColumnVector.h"
#include "TypeDescription.h"
#include <immintrin.h>
#include <avxintrin.h>

#define ENABLE_SIMD_FILTER

class PixelsFilter {
public:
    static void ApplyFilter(std::shared_ptr<ColumnVector> vector, duckdb::TableFilter &filter,
                            PixelsBitMask& filterMask,
                            std::shared_ptr<TypeDescription> type);

    template <class T, class OP>
    static int CompareAvx2(void * data, T constant);

    template <class T, class OP>
    static void TemplatedFilterOperation(std::shared_ptr<ColumnVector> vector,
                            const duckdb::Value &constant, PixelsBitMask &filter_mask,
                            std::shared_ptr<TypeDescription> type);

    template <class OP>
    static void FilterOperationSwitch(std::shared_ptr<ColumnVector> vector, duckdb::Value &constant,
                                      PixelsBitMask &filter_mask, std::shared_ptr<TypeDescription> type);

};
#endif //DUCKDB_PIXELSFILTER_H
