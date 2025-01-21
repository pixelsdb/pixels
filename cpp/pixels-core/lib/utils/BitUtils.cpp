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

/*
 * @author whz
 * @create 2024-11-27
 */
#include <stdexcept>
#include "utils/BitUtils.h"

std::vector <uint8_t> BitUtils::bitWiseCompactLE(std::vector<bool> values)
{
    return bitWiseCompactLE(values, values.size());
}

std::vector <uint8_t> BitUtils::bitWiseCompactLE(std::vector<bool> values, int length)
{
    std::vector <uint8_t> bitWiseOutput;
    int currBit = 0;
    uint8_t current = 0;

    for (int i = 0; i < length; ++i)
    {
        uint8_t v = (values[i] == true) ? 1 : 0;
        current |= v << currBit;

        currBit++;
        if (currBit == 8)
        {                                     // If we've filled a byte
            bitWiseOutput.push_back(current); // Add the byte to the output
            current = 0;                      // Reset for the next byte
            currBit = 0;                      // Reset bit counter
        }
    }

    if (currBit != 0)
    {
        bitWiseOutput.push_back(current);
    }

    return bitWiseOutput; // Return the compacted byte vector
}

std::vector <uint8_t> BitUtils::bitWiseCompactBE(std::vector<bool> values)
{
    return bitWiseCompactBE(values, values.size());
}

std::vector <uint8_t> BitUtils::bitWiseCompactBE(std::vector<bool> values, int length)
{
    std::vector <uint8_t> bitWiseOutput;
    int bitsLeft = 8;
    uint8_t current = 0;

    for (auto i = 0; i < length; ++i)
    {
        uint8_t v = (values[i] == true) ? 1 : 0;
        bitsLeft--;
        current |= v << bitsLeft;

        if (bitsLeft == 0)
        {
            bitWiseOutput.push_back(current);
            current = 0;
            bitsLeft = 8;
        }
    }

    if (bitsLeft != 8)
    {
        bitWiseOutput.push_back(current);
    }

    return bitWiseOutput;
}

std::vector <uint8_t> BitUtils::bitWiseCompact(bool *values, int length, ByteOrder byteOrder)
{
    if (byteOrder == ByteOrder::PIXELS_BIG_ENDIAN)
    {
        return bitWiseCompactBE(values, length);
    }
    else
    {
        return bitWiseCompactLE(values, length);
    }
}

std::vector <uint8_t> bitWiseCompactLE(bool *values, int length)
{
    if (!values || length < 0)
    {
        throw std::runtime_error("Invalid input: values must not be null and length must be non-negative.");
    }

    std::vector <uint8_t> bitWiseOutput;
    int currBit = 0;
    uint8_t current = 0;

    for (int i = 0; i < length; i++)
    {
        uint8_t v = values[i] ? 1 : 0;
        current |= v << currBit;

        currBit++;
        if (currBit == 8)
        {                                     // If we've filled a byte
            bitWiseOutput.push_back(current); // Add the byte to the output
            current = 0;                      // Reset for the next byte
            currBit = 0;                      // Reset bit counter
        }
    }

    if (currBit != 0)
    {
        bitWiseOutput.push_back(current);
    }

    return bitWiseOutput; // Return the compacted byte vector
}

std::vector <uint8_t> bitWiseCompactBE(bool *values, int length)
{
    if (values == nullptr || length < 0)
    {
        throw std::runtime_error("Invalid input: values must not be null and length must be non-negative.");
    }

    std::vector <uint8_t> bitWiseOutput;
    int bitsLeft = 8;
    uint8_t current = 0;

    for (int i = 0; i < length; i++)
    {
        uint8_t v = values[i] ? 1 : 0;
        bitsLeft--;
        current |= v << bitsLeft;

        if (bitsLeft == 0)
        {
            bitWiseOutput.push_back(current);
            current = 0;
            bitsLeft = 8;
        }
    }

    if (bitsLeft != 8)
    {
        bitWiseOutput.push_back(current);
    }

    return bitWiseOutput;
}

std::vector <uint8_t> BitUtils::bitWiseCompact(std::vector<bool> values, ByteOrder byteOrder)
{
    return bitWiseCompact(values, values.size(), byteOrder);
}

std::vector <uint8_t> BitUtils::bitWiseCompact(std::vector<bool> values, int length, ByteOrder byteOrder)
{
    if (byteOrder == ByteOrder::PIXELS_BIG_ENDIAN)
    {
        return bitWiseCompactBE(values, length);
    }
    else
    {
        return bitWiseCompactLE(values, length);
    }
}

std::vector <uint8_t> BitUtils::bitWiseCompact(std::vector <uint8_t> values, int length, ByteOrder byteOrder)
{
    if (byteOrder == ByteOrder::PIXELS_BIG_ENDIAN)
    {
        return bitWiseCompactBE(values, length);
    }
    else
    {
        return bitWiseCompactLE(values, length);
    }
}

std::vector <uint8_t> BitUtils::bitWiseCompactBE(std::vector <uint8_t> values, int length)
{
    std::vector <uint8_t> bitWiseOutput;
    // Issue #99: remove to improve performance.
    // int bitsToWrite = 1;
    int bitsLeft = 8;
    uint8_t current = 0;
    for (int i = 0; i < length; i++)
    {
        auto v = values[i];
        bitsLeft--; // -= bitsToWrite;
        current |= v << bitsLeft;
        if (bitsLeft == 0)
        {
            bitWiseOutput.emplace_back(current);
            current = 0;
            bitsLeft = 8;
        }
    }

    if (bitsLeft != 8)
    {
        bitWiseOutput.emplace_back(current);
    }

    return bitWiseOutput;
}

std::vector <uint8_t> BitUtils::bitWiseCompactLE(std::vector <uint8_t> values, int length)
{
    std::vector <uint8_t> bitWiseOutput;
    int currBit = 0;
    uint8_t current = 0;

    for (int i = 0; i < length; i++)
    {
        auto v = values[i];
        current |= v << currBit;
        currBit++;
        if (currBit == 8)
        {
            bitWiseOutput.emplace_back(current);
            current = 0;
            currBit = 0;
        }
    }

    if (currBit != 0)
    {
        bitWiseOutput.emplace_back(current);
    }

    return bitWiseOutput;
}

std::vector <uint8_t> BitUtils::bitWiseCompactLE(bool *values, int length)
{
    if (!values || length < 0)
    {
        throw std::runtime_error("Invalid input: values must not be null and length must be non-negative.");
    }

    std::vector <uint8_t> bitWiseOutput;
    int currBit = 0;
    uint8_t current = 0;

    for (int i = 0; i < length; i++)
    {
        uint8_t v = values[i] ? 1 : 0;
        current |= v << currBit;

        currBit++;
        if (currBit == 8)
        {                                     // If we've filled a byte
            bitWiseOutput.push_back(current); // Add the byte to the output
            current = 0;                      // Reset for the next byte
            currBit = 0;                      // Reset bit counter
        }
    }

    if (currBit != 0)
    {
        bitWiseOutput.push_back(current);
    }

    return bitWiseOutput; // Return the compacted byte vector
}

std::vector <uint8_t> BitUtils::bitWiseCompactBE(bool *values, int length)
{
    if (values == nullptr || length < 0)
    {
        throw std::runtime_error("Invalid input: values must not be null and length must be non-negative.");
    }

    std::vector <uint8_t> bitWiseOutput;
    int bitsLeft = 8;
    uint8_t current = 0;

    for (int i = 0; i < length; i++)
    {
        uint8_t v = values[i] ? 1 : 0;
        bitsLeft--;
        current |= v << bitsLeft;

        if (bitsLeft == 0)
        {
            bitWiseOutput.push_back(current);
            current = 0;
            bitsLeft = 8;
        }
    }

    if (bitsLeft != 8)
    {
        bitWiseOutput.push_back(current);
    }

    return bitWiseOutput;
}
