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
    flag.store(0, std::memory_order_relaxed);
    tileVisibilities = new TileVisibility[tileCount];
}

RGVisibility::~RGVisibility() {
    delete[] tileVisibilities;
}

void RGVisibility::beginRGAccess() {
    while (true) {
        uint32_t v = flag.load(std::memory_order_acquire);
        uint32_t accessCount = v & ACCESS_MASK;

        if (accessCount >= MAX_ACCESS_COUNT) {
            throw std::runtime_error("Reaches the max concurrent access count.");
        }

        if ((v & GC_MASK) > 0 ||
            !flag.compare_exchange_strong(v, v + ACCESS_INC, std::memory_order_acq_rel)) {
            // We failed to get gc lock or increase access count.
            if ((v & GC_MASK) > 0) {
                // if there is an existing gc, sleep for 10ms.
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            continue;
        }
        break;
    }
}

void RGVisibility::endRGAccess() {
    uint32_t v = flag.load(std::memory_order_acquire);
    while((v & ACCESS_MASK) > 0) {
        if (flag.compare_exchange_strong(v, v - ACCESS_INC, std::memory_order_acq_rel)) {
            break;
        }
        v = flag.load(std::memory_order_acquire);
    }
}

void RGVisibility::collectRGGarbage(uint64_t timestamp) {
    // Set the gc flag.
    flag.store(flag.load(std::memory_order_acquire) | GC_MASK, std::memory_order_release);

    // Wait for all access to end.
    while (true) {
        uint32_t v = flag.load(std::memory_order_acquire);
        if ((v & ACCESS_MASK) == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    assert((flag.load(std::memory_order_acquire) & GC_MASK) > 0);
    assert((flag.load(std::memory_order_acquire) & ACCESS_MASK) == 0);

    // Garbage collect.
    for (uint64_t i = 0; i < tileCount; i++) {
        tileVisibilities[i].collectTileGarbage(timestamp);
    }

    // Clear the gc flag.
    flag.store(flag.load(std::memory_order_acquire) & ~GC_MASK, std::memory_order_release);
}

TileVisibility* RGVisibility::getTileVisibility(uint64_t rowId) const {
    uint64_t tileIndex = rowId / VISIBILITY_RECORD_CAPACITY;
    if (tileIndex >= tileCount) {
        throw std::runtime_error("Row id is out of range.");
    }
    return &tileVisibilities[tileIndex];
}

void RGVisibility::deleteRGRecord(uint64_t rowId, uint64_t timestamp) {
    try {
        beginRGAccess();
        TileVisibility* tileVisibility = getTileVisibility(rowId);
        tileVisibility->deleteTileRecord(rowId % VISIBILITY_RECORD_CAPACITY, timestamp);
        endRGAccess();
    }
    catch (const std::runtime_error& e) {
        endRGAccess();
        throw std::runtime_error("Failed to delete record: " + std::string(e.what()));
    }
}

uint64_t* RGVisibility::getRGVisibilityBitmap(uint64_t timestamp) {
    beginRGAccess();
    uint64_t* bitmap = new uint64_t[tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY];
    memset(bitmap, 0, tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY * sizeof(uint64_t));

    try {
        for (uint64_t i = 0; i < tileCount; i++) {
            tileVisibilities[i].getTileVisibilityBitmap(timestamp, bitmap + i * BITMAP_SIZE_PER_TILE_VISIBILITY);
        }
        endRGAccess();
        return bitmap;
    } catch (const std::runtime_error& e) {
        delete[] bitmap;
        endRGAccess();
        throw std::runtime_error("Failed to get visibility bitmap: " + std::string(e.what()));
    }
}

uint64_t RGVisibility::getBitmapSize() const {
    return tileCount * BITMAP_SIZE_PER_TILE_VISIBILITY;
}
