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
#include "RGVisibility.h"
#include <stdexcept>
#include <cstring>
#include <thread>

// Validates before allocation: any throw leaves tileVisibilities as nullptr,
// so the incomplete constructor does not invoke the destructor (no memory leak).
template<size_t CAPACITY>
RGVisibility<CAPACITY>::RGVisibility(uint64_t rgRecordNum, uint64_t timestamp,
                                      const std::vector<uint64_t>* initialBitmap)
    : tileCount((rgRecordNum + VISIBILITY_RECORD_CAPACITY - 1) / VISIBILITY_RECORD_CAPACITY),
      tileVisibilities(nullptr) {
    if (initialBitmap && initialBitmap->size() < tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY)
        throw std::invalid_argument("Initial bitmap size is too small for the given record number.");

    tileVisibilities = static_cast<TileVisibility<CAPACITY>*>(
        operator new[](tileCount * sizeof(TileVisibility<CAPACITY>)));

    for (uint64_t i = 0; i < tileCount; ++i)
        new (&tileVisibilities[i]) TileVisibility<CAPACITY>(
            timestamp,
            initialBitmap ? initialBitmap->data() + i * BITMAP_SIZE_PER_TILE_VISIBILITY : nullptr);
}

template<size_t CAPACITY>
RGVisibility<CAPACITY>::~RGVisibility() {
    for (uint64_t i = 0; i < tileCount; ++i) {
        tileVisibilities[i].~TileVisibility();
    }
    operator delete[](tileVisibilities);
}

template<size_t CAPACITY>
std::vector<uint64_t> RGVisibility<CAPACITY>::collectRGGarbage(uint64_t timestamp) {
    size_t totalWords = tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
    std::vector<uint64_t> rgSnapshot(totalWords, 0);
    for (uint32_t t = 0; t < tileCount; t++) {
        tileVisibilities[t].collectTileGarbage(timestamp,
            rgSnapshot.data() + t * BITMAP_SIZE_PER_TILE_VISIBILITY);
    }
    return rgSnapshot;
}

template<size_t CAPACITY>
TileVisibility<CAPACITY>* RGVisibility<CAPACITY>::getTileVisibility(uint32_t rowId) const {
    uint32_t tileIndex = rowId / VISIBILITY_RECORD_CAPACITY;
    if (tileIndex >= tileCount) {
        throw std::runtime_error("Row id is out of range.");
    }
    return &tileVisibilities[tileIndex];
}

template<size_t CAPACITY>
void RGVisibility<CAPACITY>::deleteRGRecord(uint32_t rowId, uint64_t timestamp) {
    TileVisibility<CAPACITY>* tileVisibility = getTileVisibility(rowId);
    tileVisibility->deleteTileRecord(rowId % VISIBILITY_RECORD_CAPACITY, timestamp);
}

template<size_t CAPACITY>
uint64_t* RGVisibility<CAPACITY>::getRGVisibilityBitmap(uint64_t timestamp) {
    // TileVisibility::getTileVisibilityBitmap uses Epoch protection internally
    size_t len = tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
    size_t byteSize = len * sizeof(uint64_t);
    auto* bitmap = new uint64_t[len];
    pixels::g_retina_tracked_memory.fetch_add(byteSize, std::memory_order_relaxed);
    memset(bitmap, 0, byteSize);

    for (uint64_t i = 0; i < tileCount; i++) {
        tileVisibilities[i].getTileVisibilityBitmap(timestamp, bitmap + i * BITMAP_SIZE_PER_TILE_VISIBILITY);
    }
    return bitmap;
}

template<size_t CAPACITY>
uint64_t RGVisibility<CAPACITY>::getBitmapSize() const {
    return tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
}

// Explicit Instantiations for JNI use
template class RGVisibility<RETINA_CAPACITY>;
