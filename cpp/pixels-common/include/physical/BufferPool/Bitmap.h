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
 * @create 2025-07-30
 */

#ifndef BITMAP_H
#define BITMAP_H
#include "exception/InvalidArgumentException.h"
#include <cstddef>
#include <vector>
#include <iostream>

class Bitmap
{
public:
    explicit Bitmap(size_t size) : bits((size + 7) / 8, 0), numBits(size)
    {
    }

    void set(size_t index)
    {
        if (index >= numBits)
        {
            throw InvalidArgumentException("Bitmap::set: index out of range");
        }
        bits[index / 8] |= (1 << (index % 8));
    }

    void clear(size_t index)
    {
        if (index >= numBits)
        {
            throw InvalidArgumentException("Bitmap::clear: index out of range");
        }
        bits[index / 8] &= ~(1 << (index % 8));
    }

    bool test(size_t index) const
    {
        if (index >= numBits)
        {
            throw InvalidArgumentException("Bitmap::test: index out of range");
        }
        return bits[index / 8] & (1 << (index % 8));
    }

    size_t size() const
    {
        return numBits;
    }

    void print() const
    {
        for (size_t i = 0; i < numBits; ++i)
        {
            std::cout << (test(i) ? '1' : '0');
            if ((i + 1) % 8 == 0)
            {
                std::cout << ' ';
            }
        }
        std::cout << '\n';
    }

private:
    std::vector<uint8_t> bits;
    size_t numBits;
};

#endif // BITMAP_H
