//
// Created by whz on 3/20/25.
//

#ifndef DUCKDB_ENDIANNESS_H
#define DUCKDB_ENDIANNESS_H
#include <cstdint>

class Endianness{
public:
  static bool isLittleEndian(){
    uint16_t num=1;
    return (*reinterpret_cast<char*>(&num)==1);
  };
  static int64_t convertEndianness(int64_t value) {
    int64_t result = 0;
    for (int i = 0; i < 8; ++i) {
      result |= static_cast<int64_t>((value >> (i * 8)) & 0xFF) << ((7 - i) * 8);
    }
    return result;
  }

};




#endif // DUCKDB_ENDIANNESS_H
