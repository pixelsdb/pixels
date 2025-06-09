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
#ifndef PIXELS_RETINA_VISIBILITY_H
#define PIXELS_RETINA_VISIBILITY_H

#define RECORDS_PER_VISIBILITY 256
#define BITMAP_ARRAY_SIZE 4
#define CHECKPOINT_SIZE 32
#define GET_BITMAP_BIT(bitmap, rowId) (((bitmap)[(rowId) / 64] >> ((rowId) % 64)) & 1ULL)
#define SET_BITMAP_BIT(bitmap, rowId) ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#define CLEAR_BITMAP_BIT(bitmap, rowId) ((bitmap)[(rowId) / 64] &= ~(1ULL << ((rowId) % 64)))

#include <vector>
#include <cstdint>
#include <mutex>

class Visibility {
public:
    Visibility();
    ~Visibility();

    void getVisibilityBitmap(std::uint64_t epochTs, std::uint64_t *visibilityBitmap);
    void deleteRecord(int rowId, std::uint64_t epochTs);
    void createNewEpoch(std::uint64_t epochTs);
    void cleanEpochArrAndPatchArr(std::uint64_t cleanUpToEpochTs);

private:
    struct EpochEntry {
        std::uint64_t timestamp;
        std::size_t patchStartIndex;
        std::size_t patchEndIndex;
    };

    int8_t allValue;
    std::uint64_t *intendDeleteBitmap;
    std::uint64_t *actualDeleteBitmap;
    std::vector<std::uint8_t> patchArr;
    std::vector<EpochEntry> epochArr;
    std::mutex mutex_;

    std::size_t findEpoch(std::uint64_t epochTs) const;
};

#endif //PIXELS_RETINA_VISIBILITY_H
