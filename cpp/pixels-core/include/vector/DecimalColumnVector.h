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
#include "duckdb/common/types.hpp"

using PhysicalType = duckdb::PhysicalType;
class DecimalColumnVector : public ColumnVector
{
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
  void *current() override;
  int getPrecision();
  int getScale();

  void add(std::string &value) override;
  void add(long value) override;
  void ensureSize(uint64_t size, bool preserveData) override;
};
#endif //PIXELS_DECIMALCOLUMNVECTOR_H
