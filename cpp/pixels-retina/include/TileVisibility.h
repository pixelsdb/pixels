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
#ifndef BITMAP_WORDS
#define BITMAP_WORDS(cap) (((cap) + 63) / 64)
#endif
#include "RetinaBase.h"
#include <atomic>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <vector>

// rowId supports up to 65535, timestamp uses 48 bits
inline uint64_t makeDeleteIndex(uint16_t rowId, uint64_t ts) {
    return (static_cast<uint64_t>(rowId) << 48 | (ts & 0x0000FFFFFFFFFFFFULL));
}

inline uint16_t extractRowId(uint64_t raw) {
    return static_cast<uint16_t>(raw >> 48);
}

inline uint64_t extractTimestamp(uint64_t raw) {
    return (raw & 0x0000FFFFFFFFFFFFULL);
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
template<size_t CAPACITY>
struct VersionedData : public pixels::RetinaBase<VersionedData<CAPACITY>> {
    static constexpr size_t NUM_WORDS = BITMAP_WORDS(CAPACITY);
    uint64_t baseBitmap[NUM_WORDS];
    uint64_t baseTimestamp;
    DeleteIndexBlock* head;

    VersionedData() : baseTimestamp(0), head(nullptr) {
        std::memset(baseBitmap, 0, sizeof(baseBitmap));
    }

    VersionedData(uint64_t ts, const uint64_t* bitmap, DeleteIndexBlock* h)
        : baseTimestamp(ts), head(h) {
        std::memcpy(baseBitmap, bitmap, NUM_WORDS * sizeof(uint64_t));
    }
};

/**
 * RetiredVersion - Tracks a retired version for epoch-based reclamation
 */
template<size_t CAPACITY>
struct RetiredVersion : public pixels::RetinaBase<RetiredVersion<CAPACITY>> {
    VersionedData<CAPACITY>* data; // Fixed: added <CAPACITY>
    DeleteIndexBlock* blocksToDelete;
    uint64_t retireEpoch;

    RetiredVersion(VersionedData<CAPACITY>* d, DeleteIndexBlock* b, uint64_t e)
        : data(d), blocksToDelete(b), retireEpoch(e) {}
};

template<size_t CAPACITY>
class TileVisibility : public pixels::RetinaBase<TileVisibility<CAPACITY>> {
    static constexpr size_t NUM_WORDS = BITMAP_WORDS(CAPACITY);
public:
    TileVisibility();
    TileVisibility(uint64_t ts, const uint64_t* bitmap);
    ~TileVisibility() override;
    void deleteTileRecord(uint16_t rowId, uint64_t ts);
    void getTileVisibilityBitmap(uint64_t ts, uint64_t* outBitmap) const;
    void collectTileGarbage(uint64_t ts);

  private:
    TileVisibility(const TileVisibility &) = delete;
    TileVisibility &operator=(const TileVisibility &) = delete;

    void reclaimRetiredVersions();

    std::atomic<VersionedData<CAPACITY>*> currentVersion;
    std::atomic<DeleteIndexBlock *> tail;
    std::atomic<size_t> tailUsed;
    std::vector<RetiredVersion<CAPACITY>> retired;  // Protected by GC (single writer)
};

#endif // PIXELS_RETINA_TILE_VISIBILITY_H
