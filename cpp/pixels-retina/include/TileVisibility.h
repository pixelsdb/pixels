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
#include <utility>
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
    DeleteIndexBlock* head; // Delete chain head, part of the version

    // timestamp defaults to 0; bitmap defaults to all-zeros.
    explicit VersionedData(uint64_t ts = 0, const uint64_t* bitmap = nullptr, DeleteIndexBlock* h = nullptr)
        : baseTimestamp(ts), head(h) {
        if (bitmap)
            std::memcpy(baseBitmap, bitmap, NUM_WORDS * sizeof(uint64_t));
        else
            std::memset(baseBitmap, 0, sizeof(baseBitmap));
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
    // timestamp defaults to 0; bitmap defaults to all-zeros.
    explicit TileVisibility(uint64_t timestamp = 0, const uint64_t* bitmap = nullptr);
    ~TileVisibility() override;
    void deleteTileRecord(uint16_t rowId, uint64_t ts);
    void getTileVisibilityBitmap(uint64_t ts, uint64_t* outBitmap) const;
    void collectTileGarbage(uint64_t ts, uint64_t* gcSnapshotBitmap);
    void exportChainItemsAfter(uint32_t tileId, uint64_t safeGcTs,
        std::vector<std::pair<uint32_t, uint64_t>>& gcChainItems) const;
    void importDeletionItems(std::vector<uint64_t>& bucket);

  private:
    TileVisibility(const TileVisibility &) = delete;
    TileVisibility &operator=(const TileVisibility &) = delete;

    void reclaimRetiredVersions();

    std::atomic<VersionedData<CAPACITY>*> currentVersion;
    std::atomic<DeleteIndexBlock *> tail;
    std::atomic<size_t> tailUsed;
    
    // Retired versions awaiting epoch-based reclamation.  Only the GC thread
    // (collectTileGarbage / reclaimRetiredVersions) reads and writes this vector,
    // so no locking is needed.
    std::vector<RetiredVersion<CAPACITY>> retired;

    // Lock-free staging slot between deleteTileRecord (CDC threads) and GC.
    // deleteTileRecord's empty-chain path replaces currentVersion but cannot
    // write `retired` directly — that would race with the GC thread.  Instead
    // it atomically stores oldVer here.  The GC thread drains this slot at the
    // start of collectTileGarbage, moving it into `retired` with a proper epoch.
    // Flow:  deleteTileRecord → pendingRetire.store → collectTileGarbage →
    //        pendingRetire.exchange(nullptr) → retired.emplace_back → reclaimRetiredVersions
    // At most one version is pending per GC cycle (the empty-chain path fires
    // at most once between consecutive GC compactions).
    std::atomic<VersionedData<CAPACITY>*> pendingRetire{nullptr};
};

#endif // PIXELS_RETINA_TILE_VISIBILITY_H
