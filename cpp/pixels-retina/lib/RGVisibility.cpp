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

RGVisibility::RGVisibility(uint64_t rgRecordNum)
    : tileCount((rgRecordNum + VISIBILITY_RECORD_CAPACITY - 1) / VISIBILITY_RECORD_CAPACITY) {
    size_t allocSize = tileCount * sizeof(TileVisibility); // Define size
    void* rawMemory = operator new[](allocSize);
    tileVisibilities = static_cast<TileVisibility*>(rawMemory);
    for (uint64_t i = 0; i < tileCount; ++i) {
        new (&tileVisibilities[i]) TileVisibility();
    }
}

RGVisibility::RGVisibility(uint64_t rgRecordNum, uint64_t timestamp, const std::vector<uint64_t>& initialBitmap)
    : tileCount((rgRecordNum + VISIBILITY_RECORD_CAPACITY - 1) / VISIBILITY_RECORD_CAPACITY) {
    size_t allocSize = tileCount * sizeof(TileVisibility); // Define size
    void* rawMemory = operator new[](allocSize);

    // Ensure bitmap size matches
    if (initialBitmap.size() < tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY) {
        operator delete[](rawMemory);
        throw std::runtime_error("Initial bitmap size is too small for the given record number.");
    }

    tileVisibilities = static_cast<TileVisibility*>(rawMemory);
    for (uint64_t i = 0; i < tileCount; ++i) {
        const uint64_t* tileBitmap = &initialBitmap[i * BITMAP_SIZE_PER_TILE_VISIBILITY];
        new (&tileVisibilities[i]) TileVisibility(timestamp, tileBitmap);
    }
}

RGVisibility::~RGVisibility() {
    for (uint64_t i = 0; i < tileCount; ++i) {
        tileVisibilities[i].~TileVisibility();
    }
    operator delete[](tileVisibilities);
}

void RGVisibility::collectRGGarbage(uint64_t timestamp) {
    // TileVisibility::collectTileGarbage uses COW + Epoch, so it's safe to call concurrently
    for (uint64_t i = 0; i < tileCount; i++) {
        tileVisibilities[i].collectTileGarbage(timestamp);
    }
}

TileVisibility* RGVisibility::getTileVisibility(uint32_t rowId) const {
    uint32_t tileIndex = rowId / VISIBILITY_RECORD_CAPACITY;
    if (tileIndex >= tileCount) {
        throw std::runtime_error("Row id is out of range.");
    }
    return &tileVisibilities[tileIndex];
}

void RGVisibility::deleteRGRecord(uint32_t rowId, uint64_t timestamp) {
    // TileVisibility::deleteTileRecord is lock-free and concurrent-safe
    TileVisibility* tileVisibility = getTileVisibility(rowId);
    tileVisibility->deleteTileRecord(rowId % VISIBILITY_RECORD_CAPACITY, timestamp);
}

uint64_t* RGVisibility::getRGVisibilityBitmap(uint64_t timestamp) {
    size_t len = tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
    size_t byteSize = len * sizeof(uint64_t);
    // TileVisibility::getTileVisibilityBitmap uses Epoch protection internally
    auto* bitmap = new uint64_t[len];
    pixels::g_retina_tracked_memory.fetch_add(byteSize, std::memory_order_relaxed);
    memset(bitmap, 0, tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY * sizeof(uint64_t));

    for (uint64_t i = 0; i < tileCount; i++) {
        tileVisibilities[i].getTileVisibilityBitmap(timestamp, bitmap + i * BITMAP_SIZE_PER_TILE_VISIBILITY);
    }
    return bitmap;
}

uint64_t RGVisibility::getBitmapSize() const {
    return tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
}
