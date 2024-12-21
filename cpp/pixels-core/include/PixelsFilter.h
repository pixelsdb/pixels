/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

/*
 * @author liyu
 * @create 2023-06-23
 */
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
