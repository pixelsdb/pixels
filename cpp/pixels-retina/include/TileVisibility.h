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
#ifndef PIXELS_RETINA_TILE_VISIBILITY_H
#define PIXELS_RETINA_TILE_VISIBILITY_H

#ifndef SET_BITMAP_BIT
#define SET_BITMAP_BIT(bitmap, rowId) \
    ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#endif

#include "RetinaBase.h"
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

inline uint64_t makeDeleteIndex(uint8_t rowId, uint64_t ts) {
    return (static_cast<uint64_t>(rowId) << 56 | (ts & 0x00FFFFFFFFFFFFFFULL));
}

inline uint8_t extractRowId(uint64_t raw) {
    return static_cast<uint8_t>(raw >> 56);
}

inline uint64_t extractTimestamp(uint64_t raw) {
    return (raw & 0x00FFFFFFFFFFFFFFULL);
}

struct DeleteIndexBlock : public pixels::RetinaBase<DeleteIndexBlock> {
    static constexpr size_t BLOCK_CAPACITY = 8;
    uint64_t items[BLOCK_CAPACITY] = {0};
    std::atomic<DeleteIndexBlock *> next{nullptr};
};

/**
 * VersionedData - A versioned snapshot of the base state
 * Used for Copy-on-Write during garbage collection
 * IMPORTANT: head is part of the version to ensure atomic visibility
 */
struct VersionedData : public pixels::RetinaBase<VersionedData> {
    uint64_t baseBitmap[4];
    uint64_t baseTimestamp;
    DeleteIndexBlock* head;  // Delete chain head, part of the version
    
    VersionedData() : baseTimestamp(0), head(nullptr) {
        baseBitmap[0] = baseBitmap[1] = baseBitmap[2] = baseBitmap[3] = 0;
    }
    
    VersionedData(uint64_t ts, const uint64_t bitmap[4], DeleteIndexBlock* h)
        : baseTimestamp(ts), head(h) {
        baseBitmap[0] = bitmap[0];
        baseBitmap[1] = bitmap[1];
        baseBitmap[2] = bitmap[2];
        baseBitmap[3] = bitmap[3];
    }
};

/**
 * RetiredVersion - Tracks a retired version for epoch-based reclamation
 */
struct RetiredVersion : public pixels::RetinaBase<RetiredVersion> {
    VersionedData* data;
    DeleteIndexBlock* blocksToDelete;  // Head of the chain to delete
    uint64_t retireEpoch;
    
    RetiredVersion(VersionedData* d, DeleteIndexBlock* b, uint64_t e)
        : data(d), blocksToDelete(b), retireEpoch(e) {}
};

class TileVisibility : public pixels::RetinaBase<TileVisibility>  {
  public:
    TileVisibility();
    TileVisibility(uint64_t ts, const uint64_t bitmap[4]);
    ~TileVisibility() override;
    void deleteTileRecord(uint8_t rowId, uint64_t ts);
    void getTileVisibilityBitmap(uint64_t ts, uint64_t outBitmap[4]) const;
    void collectTileGarbage(uint64_t ts);

  private:
    TileVisibility(const TileVisibility &) = delete;
    TileVisibility &operator=(const TileVisibility &) = delete;

    void reclaimRetiredVersions();
    
    std::atomic<VersionedData*> currentVersion;
    std::atomic<DeleteIndexBlock *> tail;
    std::atomic<size_t> tailUsed;
    std::vector<RetiredVersion> retired;  // Protected by GC (single writer)
};

#endif // PIXELS_RETINA_TILE_VISIBILITY_H
