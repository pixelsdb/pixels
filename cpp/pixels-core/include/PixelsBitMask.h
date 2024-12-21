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
 * @create 2023-07-06
 */
#ifndef DUCKDB_PIXELSBITMASK_H
#define DUCKDB_PIXELSBITMASK_H

#include <bitset>
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

#include "vector/ColumnVector.h"
#include "TypeDescription.h"

class PixelsBitMask {
public:
    uint8_t * mask;
    long maskLength;
    long arrayLength;
    PixelsBitMask(long length);
    PixelsBitMask(PixelsBitMask & other);
    ~PixelsBitMask();
    void Or(PixelsBitMask & other);
    void And(PixelsBitMask & other);
    void Or(long index, uint8_t value);
    void And(long index, uint8_t value);
    bool isNone();
    void set();
    void set(long index, uint8_t value);
    void setByteAligned(long index, uint8_t value);
    uint8_t get(long index);
};

#endif //DUCKDB_PIXELSBITMASK_H
