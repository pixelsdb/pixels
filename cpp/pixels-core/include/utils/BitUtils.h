/*
 * Copyright 2024 PixelsDB.
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

