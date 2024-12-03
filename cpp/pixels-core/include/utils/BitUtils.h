//
// Created by whz on 11/27/24.
//

#ifndef PIXELS_BITUTILS_H
#define PIXELS_BITUTILS_H
#include <vector>
#include <cstdint>
#include "physical/natives/ByteOrder.h"

class BitUtils
{
private:
    BitUtils() {};
    static std::vector<uint8_t> bitWiseCompactLE(bool *values, int length);
    static std::vector<uint8_t> bitWiseCompactLE(std::vector<bool> values);
    static std::vector<uint8_t> bitWiseCompactLE(std::vector<bool> values, int length);
    static std::vector<uint8_t> bitWiseCompactBE(bool *values, int length);
    static std::vector<uint8_t> bitWiseCompactBE(std::vector<bool> values);
    static std::vector<uint8_t> bitWiseCompactBE(std::vector<bool> values, int length);

public:
    static std::vector<uint8_t> bitWiseCompact(bool *values, int length, ByteOrder byteOrder);
    static std::vector<uint8_t> bitWiseCompact(std::vector<bool> values, ByteOrder byteOrder);
    static std::vector<uint8_t> bitWiseCompact(std::vector<bool> values, int length, ByteOrder byteOrder);

public:
    static std::vector<uint8_t> bitWiseCompact(std::vector<uint8_t> values, int length, ByteOrder byteOrder);

private:
    static std::vector<uint8_t> bitWiseCompactBE(std::vector<uint8_t> values, int length);
    static std::vector<uint8_t> bitWiseCompactLE(std::vector<uint8_t> values, int length);
};

#endif // PIXELS_BITUTILS_H

