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
 * @create 2025-01-13
 */
#ifndef DYNAMICINTARRAY_H
#define DYNAMICINTARRAY_H

#include <iostream>
#include <cstring>

class DynamicIntArray {
public:
  DynamicIntArray();
  DynamicIntArray(int chunkSize);

  int get(int index);

  void set(int index, int value);

  void increment(int index, int value);

  void add(int value);

  int size();

  void clear();

  std::string toString();

  int* toArray();

private:
  static const int DEFAULT_CHUNKSIZE = 8 * 1024;
  static const int INIT_CHUNKS = 256;
  int chunkSize;
  int** data;
  int length;
  int initializedChunks;

  void grow(int chunkIndex);
};

#endif //DYNAMICINTARRAY_H
