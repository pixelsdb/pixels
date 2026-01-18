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
#include "RetinaBase.h"
#include "TileVisibility.h"
#include <vector>

template<size_t CAPACITY>
class RGVisibility : public pixels::RetinaBase<RGVisibility<CAPACITY>> {
public:
    explicit RGVisibility(uint64_t rgRecordNum);
    explicit RGVisibility(uint64_t rgRecordNum, uint64_t timestamp, const std::vector<uint64_t>& initialBitmap);
    ~RGVisibility() override;

    void deleteRGRecord(uint32_t rowId, uint64_t timestamp);
    uint64_t* getRGVisibilityBitmap(uint64_t timestamp);

    void collectRGGarbage(uint64_t timestamp);

    uint64_t getBitmapSize() const;

private:
    static constexpr uint32_t VISIBILITY_RECORD_CAPACITY = CAPACITY;
    static constexpr uint32_t BITMAP_SIZE_PER_TILE_VISIBILITY = BITMAP_WORDS(CAPACITY);

    TileVisibility<CAPACITY>* tileVisibilities;
    const uint64_t tileCount;

    TileVisibility<CAPACITY>* getTileVisibility(uint32_t rowId) const;
};

static_assert(RETINA_CAPACITY > 0 && RETINA_CAPACITY % 64 == 0,
              "RETINA_CAPACITY must be a multiple of 64.");
using RGVisibilityInstance = RGVisibility<RETINA_CAPACITY>;
#endif //RG_VISIBILITY_H
