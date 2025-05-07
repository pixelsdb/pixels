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
 * @create 25-03-20
 */

#ifndef DUCKDB_ENDIANNESS_H
#define DUCKDB_ENDIANNESS_H

#include <cstdint>

class Endianness
{
public:
    static bool isLittleEndian()
    {
        uint16_t num = 1;
        return (*reinterpret_cast<char *>(&num) == 1);
    };

    static int64_t convertEndianness(int64_t value)
    {
        int64_t result = 0;
        for (int i = 0; i < 8; ++i)
        {
            result |= static_cast<int64_t>((value >> (i * 8)) & 0xFF) << ((7 - i) * 8);
        }
        return result;
    }
};


#endif // DUCKDB_ENDIANNESS_H
