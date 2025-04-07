/*
 * Copyright 2025 PixelsDB.
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
 * @author whz
 * @create 2025-04-01
 */
#include <algorithm>
#include <vector/IntColumnVector.h>

IntColumnVector::IntColumnVector(uint64_t len, bool encoding, bool isLong)
    : ColumnVector(len, encoding) {

  posix_memalign(reinterpret_cast<void **>(&intVector), 32,
                 len * sizeof(int32_t));

  memoryUsage += (long)sizeof(long) * len;
}

void IntColumnVector::close() {
  if (!closed) {
    ColumnVector::close();
    if (encoding && intVector != nullptr) {
      free(intVector);
    }
    intVector = nullptr;
  }
}

void IntColumnVector::print(int rowCount) {
  throw InvalidArgumentException("not support print longcolumnvector.");
  //    for(int i = 0; i < rowCount; i++) {
  //        std::cout<<longVector[i]<<std::endl;
  //		std::cout<<intVector[i]<<std::endl;
  //    }
}

IntColumnVector::~IntColumnVector() {
  if (!closed) {
    IntColumnVector::close();
  }
}

void *IntColumnVector::current() {
  if (intVector == nullptr) {
    return nullptr;
  } else {
    return intVector + readIndex;
  }
}

void IntColumnVector::add(std::string &value) {
  std::transform(value.begin(), value.end(), value.begin(), ::tolower);
  if (value == "true") {
    add(1);
  } else if (value == "false") {
    add(0);
  } else {
    add(std::stol(value));
  }
}

void IntColumnVector::add(bool value) { add(value ? 1 : 0); }

void IntColumnVector::add(int64_t value) {
  if (writeIndex >= length) {
    ensureSize(writeIndex * 2, true);
  }
  int index = writeIndex++;

  intVector[index] = value;

  isNull[index] = false;
}

void IntColumnVector::add(int value) {
  if (writeIndex >= length) {
    ensureSize(writeIndex * 2, true);
  }
  int index = writeIndex++;
  intVector[index] = value;
  isNull[index] = false;
}

void IntColumnVector::ensureSize(uint64_t size, bool preserveData) {
  ColumnVector::ensureSize(size, preserveData);
  if (length < size) {
    int *oldVector = intVector;
    posix_memalign(reinterpret_cast<void **>(&intVector), 32,
                   size * sizeof(int32_t));
    if (preserveData) {
      std::copy(oldVector, oldVector + length, intVector);
    }
    delete[] oldVector;
    memoryUsage += (long)sizeof(int) * (size - length);
    resize(size);
  }
}
