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

#ifndef PIXELS_VISIBILITY_H
#define PIXELS_VISIBILITY_H

#define GET_BITMAP_BIT(bitmap, rowId) (((bitmap)[(rowId) / 64] >> ((rowId) % 64)) & 1ULL)
#define SET_BITMAP_BIT(bitmap, rowId) ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#define CLEAR_BITMAP_BIT(bitmap, rowId) ((bitmap)[(rowId) / 64] &= ~(1ULL << ((rowId) % 64)))

#include <vector>
#include <cstdint>
#include <mutex>

struct EpochEntry {
    int offset;
    int timestamp;
};

class Visibility {
public:
    Visibility();
    ~Visibility();

    std::vector<uint64_t> getReadableBitmap(int timestamp);
    void deleteRow(int rowId, int timestamp);

private:
    int8_t allValue;
    uint64_t deleteBitmap1[4];
    uint64_t deleteBitmap2[4];
    std::vector<uint8_t> patchArray;
    std::vector<EpochEntry> epochArray;
    std::mutex mutex_;
};

#endif //PIXELS_VISIBILITY_H
