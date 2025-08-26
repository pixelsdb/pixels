/*
/* @author whz 
/* @create 7/30/25.
*/

#ifndef BITMAP_H
#define BITMAP_H
#include <cstddef>
#include "exception/InvalidArgumentException.h"
#include <iostream>

class Bitmap {
public:

  explicit Bitmap(size_t size)
      : bits((size + 7) / 8, 0), num_bits(size) {}

  void set(size_t index) {
    if (index >= num_bits) throw InvalidArgumentException("Bitmap::set: index out of range");
    bits[index / 8] |= (1 << (index % 8));
  }

  void clear(size_t index) {
    if (index >= num_bits) throw InvalidArgumentException("Bitmap::clear: index out of range");
    bits[index / 8] &= ~(1 << (index % 8));
  }

  bool test(size_t index) const {
    if (index >= num_bits) throw InvalidArgumentException("Bitmap::test: index out of range");
    return bits[index / 8] & (1 << (index % 8));
  }

  size_t size() const { return num_bits; }

  void print() const {
    for (size_t i = 0; i < num_bits; ++i) {
      std::cout << (test(i) ? '1' : '0');
      if ((i + 1) % 8 == 0) std::cout << ' ';
    }
    std::cout << '\n';
  }

private:
  std::vector<uint8_t> bits;
  size_t num_bits;
};

#endif //BITMAP_H
