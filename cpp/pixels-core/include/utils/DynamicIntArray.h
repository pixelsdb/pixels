//
// Created by whz on 1/13/25.
//

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
