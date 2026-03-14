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
 * @create 2023-04-05
 */
#ifndef PIXELS_DECIMALCOLUMNVECTOR_H
#define PIXELS_DECIMALCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include "PixelsTypes.h"  //引入中立类型

#pragma once
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <algorithm>

class DecimalColumnVector : public ColumnVector
{
public:
    DecimalColumnVector(uint64_t len, int precision, int scale, bool encoding);
    ~DecimalColumnVector();

    void add(long value);
    void add(const std::string& value);

    void* current();
    void close();

    int getPrecision() const { return precision_; }
    int getScale() const { return scale_; }
    void* getData() const { return vector; }
    pixels::PhysicalType getPhysicalType() const { return physical_type_; }
    size_t ElementSize() const;
    void ensureSize(uint64_t size, bool preserveData);
    void* vector;//这里改为void，之后根据大小调整
    int precision_;
    int scale_;

    pixels::PhysicalType physical_type_;
};

#endif //PIXELS_DECIMALCOLUMNVECTOR_H