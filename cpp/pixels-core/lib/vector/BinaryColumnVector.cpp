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
#include "vector/BinaryColumnVector.h"

BinaryColumnVector::BinaryColumnVector(uint64_t len, bool encoding) : ColumnVector(len, encoding)
{
  posix_memalign(reinterpret_cast<void **>(&vector), 32,
                 len * sizeof(duckdb::string_t));
  str_vec.resize(len);
  memoryUsage += (long) sizeof(uint8_t) * len;
}

void BinaryColumnVector::close()
{
  if (!closed)
  {
    ColumnVector::close();
    free(vector);
    vector = nullptr;
  }
}

void BinaryColumnVector::setRef(int elementNum, uint8_t *const &sourceBuf, int start, int length)
{
  if (elementNum >= writeIndex)
  {
    writeIndex = elementNum + 1;
  }
  this->vector[elementNum]
      = duckdb::string_t((char *) (sourceBuf + start), length);
//    std::cout<< this->vector[elementNum].GetString()<<std::endl;
  // TODO: isNull should implemented, but not now.

}

void BinaryColumnVector::print(int rowCount)
{
  throw InvalidArgumentException("not support print binarycolumnvector.");
}

BinaryColumnVector::~BinaryColumnVector()
{
  if (!closed)
  {
    BinaryColumnVector::close();
  }
}

void *BinaryColumnVector::current()
{
  if (vector == nullptr)
  {
    return nullptr;
  } else
  {
    return vector + readIndex;
  }
}

void BinaryColumnVector::add(std::string &value)
{
  size_t len = value.size();
  uint8_t *buffer = new uint8_t[len];
  std::memcpy(buffer, value.data(), len);
  add(buffer, len);
  delete[] buffer;
}

void BinaryColumnVector::add(uint8_t *v, int len)
{
  if (writeIndex >= length)
  {
    ensureSize(writeIndex * 2, true);
  }
  setVal(writeIndex++, v, 0, len);
}

void BinaryColumnVector::setVal(int elementNum, uint8_t *sourceBuf, int start, int length)
{
  vector[elementNum] = duckdb::string_t(reinterpret_cast<char *>(sourceBuf + start), length);
  isNull[elementNum] = false;
  str_vec[elementNum] = std::string(reinterpret_cast<char *>(sourceBuf + start), length);
}

void BinaryColumnVector::ensureSize(uint64_t size, bool preserveData)
{
  ColumnVector::ensureSize(size, preserveData);
  if (length < size)
  {
    duckdb::string_t *oldVector = vector;
    posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(duckdb::string_t));
    str_vec.resize(size);
    if (preserveData)
    {
      std::copy(oldVector, oldVector + length, vector);
    }
    delete[] oldVector;
    memoryUsage += (long) sizeof(duckdb::string_t) * (size - length);
    resize(size);
  }
}