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
 * @create 2023-03-17
 */
#ifndef PIXELS_BINARYCOLUMNVECTOR_H
#define PIXELS_BINARYCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include "PixelsTypes.h" 
#include <string>
#include <vector>


// BinaryColumnVector.h

class BinaryColumnVector : public ColumnVector
{
 public:
  pixels::string_t *vector;
  std::vector<std::string> str_vec;
  explicit BinaryColumnVector(uint64_t len = VectorizedRowBatch::DEFAULT_SIZE, bool encoding = false);
  ~BinaryColumnVector();
  void setRef(int elementNum, uint8_t *const &sourceBuf, int start, int length);
  void *current() override;
  void close() override;
  void print(int rowCount) override;
  void add(std::string &value) override;
  void add(uint8_t *v, int length);
  //void setVal(int elemnetNum,uint8_t* sourceBuf);
  void setVal(int elementNum, uint8_t *sourceBuf, int start, int length);
  void ensureSize(uint64_t size, bool preserveData) override;
};

#endif //PIXELS_BINARYCOLUMNVECTOR_H
