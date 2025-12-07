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
#ifndef RG_VISIBILITY_H
#define RG_VISIBILITY_H

#include "TileVisibility.h"
#include <memory>
#include <atomic>
#include <vector>

class RGVisibility {
public:
    explicit RGVisibility(uint64_t rgRecordNum);
    explicit RGVisibility(uint64_t rgRecordNum, uint64_t timestamp, const std::vector<uint64_t>& initialBitmap);
    ~RGVisibility();

    void deleteRGRecord(uint32_t rowId, uint64_t timestamp);
    uint64_t* getRGVisibilityBitmap(uint64_t timestamp);

    void collectRGGarbage(uint64_t timestamp);

    uint64_t getBitmapSize() const;

private:
    static constexpr uint32_t VISIBILITY_RECORD_CAPACITY = 256;
    static constexpr uint32_t MAX_ACCESS_COUNT = 0x007FFFFF;
    static constexpr uint32_t GC_MASK = 0xFF000000;
    static constexpr uint32_t ACCESS_MASK = 0x00FFFFFF;
    static constexpr uint32_t ACCESS_INC = 0x00000001;
    static constexpr uint32_t BITMAP_SIZE_PER_TILE_VISIBILITY = 4;
    static constexpr uint32_t RG_READ_LEASE_MS = 100;

    TileVisibility* tileVisibilities;
    const uint64_t tileCount;
    std::atomic<uint32_t> flag; // high 1 byte is the gc flag, low 3 bytes are the access count

    TileVisibility* getTileVisibility(uint32_t rowId) const;
    void beginRGAccess();
    void endRGAccess();
};

#endif //RG_VISIBILITY_H
