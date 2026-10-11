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
#include <array>
#include <memory>
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

/**
 * Controls how DELETE replay interacts with the compacted base bitmap.
 *
 * NORMAL is the live append path: the caller provides a current delete
 * timestamp and the record is appended to the chain. VERSIONED is used when
 * replay may race with READY readers; historical deletes publish a new
 * VersionedData with a folded baseBitmap. EXCLUSIVE is used during recovery. Both replay modes publish historical
 * bitmap folds with CAS, so concurrent recovery writers do not lose updates.
 */
enum class ReplayMode : uint8_t {
    NORMAL = 0,
    VERSIONED = 1,
    EXCLUSIVE = 2
};

// Blocks are immutable after publication. Only a private, unpublished block may
// be filled or linked. The newest block is at the head of the persistent chain.
struct DeleteIndexBlock : public pixels::RetinaBase<DeleteIndexBlock> {
    static constexpr size_t BLOCK_CAPACITY = 8;
    uint64_t items[BLOCK_CAPACITY] = {0};
    size_t count = 0;
    DeleteIndexBlock* next = nullptr;
};

template<size_t CAPACITY>
struct VersionedData : public pixels::RetinaBase<VersionedData<CAPACITY>> {
    static constexpr size_t NUM_WORDS = BITMAP_WORDS(CAPACITY);
    struct Bitmap : public pixels::RetinaBase<Bitmap> {
        std::array<uint64_t, NUM_WORDS> words;
        uint64_t* data() { return words.data(); }
        const uint64_t* data() const { return words.data(); }
        void fill(uint64_t value) { words.fill(value); }
    };
    // Ordinary appends share the immutable bitmap rather than copying it.
    std::shared_ptr<const Bitmap> base;
    const uint64_t* baseBitmap;
    uint64_t baseTimestamp;
    DeleteIndexBlock* head;

    explicit VersionedData(uint64_t ts = 0, const uint64_t* bitmap = nullptr,
                           DeleteIndexBlock* h = nullptr)
        : base(makeBitmap(bitmap)), baseBitmap(base->data()), baseTimestamp(ts), head(h) {}

    VersionedData(const VersionedData& old, DeleteIndexBlock* h)
        : base(old.base), baseBitmap(base->data()), baseTimestamp(old.baseTimestamp), head(h) {}

private:
    static std::shared_ptr<const Bitmap> makeBitmap(const uint64_t* bitmap) {
        auto result = std::make_shared<Bitmap>();
        if (bitmap) std::memcpy(result->data(), bitmap, NUM_WORDS * sizeof(uint64_t));
        else result->fill(0);
        return result;
    }
};

template<size_t CAPACITY>
struct RetiredVersion : public pixels::RetinaBase<RetiredVersion<CAPACITY>> {
    VersionedData<CAPACITY>* data;
    DeleteIndexBlock* blocksToDelete;
    // A copied head retires one block; a successful GC retires the entire old
    // chain, which shares no blocks with its rebuilt replacement.
    bool wholeChain;
    uint64_t retireEpoch;
    RetiredVersion* next = nullptr;

    RetiredVersion(VersionedData<CAPACITY>* d, DeleteIndexBlock* b, bool all, uint64_t e)
        : data(d), blocksToDelete(b), wholeChain(all), retireEpoch(e) {}
};

template<size_t CAPACITY>
class TileVisibility : public pixels::RetinaBase<TileVisibility<CAPACITY>> {
    static constexpr size_t NUM_WORDS = BITMAP_WORDS(CAPACITY);
 public:
    // timestamp defaults to 0; bitmap defaults to all-zeros.
    explicit TileVisibility(uint64_t timestamp = 0, const uint64_t* bitmap = nullptr);
    ~TileVisibility() override;
    void deleteTileRecord(uint16_t rowId, uint64_t ts, ReplayMode replayMode = ReplayMode::NORMAL);
    void getTileVisibilityBitmap(uint64_t ts, uint64_t* outBitmap) const;
    void collectTileGarbage(uint64_t ts, uint64_t* gcSnapshotBitmap);
    void exportChainItemsAfter(uint32_t tileId, uint64_t safeGcTs,
        std::vector<std::pair<uint32_t, uint64_t>>& gcChainItems) const;
    void importDeletionItems(std::vector<uint64_t>& bucket);

  private:
    TileVisibility(const TileVisibility &) = delete;
    TileVisibility &operator=(const TileVisibility &) = delete;

    void reclaimRetiredVersions();

    void appendDeleteChain(uint16_t rowId, uint64_t ts);

    // VERSIONED: replay with possible readers; historical deletes use COW fold.
    void deleteTileRecordVersioned(uint16_t rowId, uint64_t ts);

    // EXCLUSIVE: recovery replay without readers; historical deletes fold in place.
    void deleteTileRecordExclusive(uint16_t rowId, uint64_t ts);

    void retireVersion(VersionedData<CAPACITY>* version,
                       DeleteIndexBlock* blocks, bool wholeChain);
    static void deleteChain(DeleteIndexBlock* head);
    static DeleteIndexBlock* buildChain(const std::vector<uint64_t>& items,
                                       DeleteIndexBlock* next = nullptr);

    // The successful CAS is the publication point for bitmap, timestamp, and
    // complete chain together. Readers and writers protect old states by epoch.
    std::atomic<VersionedData<CAPACITY>*> currentVersion;
    std::atomic<RetiredVersion<CAPACITY>*> pendingRetire{nullptr};
    std::atomic<unsigned> retireCount{0};
    // This try-only guard coordinates reclamation bookkeeping, never reads or
    // publication. A busy collector is skipped; no operation waits for it.
    std::atomic_flag reclaiming = ATOMIC_FLAG_INIT;
    std::vector<RetiredVersion<CAPACITY>*> retired;
};

#endif // PIXELS_RETINA_TILE_VISIBILITY_H
