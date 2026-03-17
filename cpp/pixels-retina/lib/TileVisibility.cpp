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

#include "TileVisibility.h"
#include "EpochManager.h"

#include <cstring>
#include <stdexcept>

#include <immintrin.h>

#include "TileVisibility.h"
#include "EpochManager.h"
#include <cstring>
#include <stdexcept>
#include <immintrin.h>

template<size_t CAPACITY>
TileVisibility<CAPACITY>::TileVisibility() {
    VersionedData<CAPACITY>* initialVersion = new VersionedData<CAPACITY>();
    currentVersion.store(initialVersion, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
    tailUsed.store(0, std::memory_order_release);
}

template<size_t CAPACITY>
TileVisibility<CAPACITY>::TileVisibility(uint64_t ts, const uint64_t* bitmap) {
    VersionedData<CAPACITY>* initialVersion = new VersionedData<CAPACITY>(ts, bitmap, nullptr);
    currentVersion.store(initialVersion, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
    tailUsed.store(0, std::memory_order_release);
}

template<size_t CAPACITY>
TileVisibility<CAPACITY>::~TileVisibility() {
    VersionedData<CAPACITY>* ver = currentVersion.load(std::memory_order_acquire);
    if (ver) {
        DeleteIndexBlock *blk = ver->head;
        while (blk) {
            DeleteIndexBlock *next = blk->next.load(std::memory_order_acquire);
            delete blk;
            blk = next;
        }
        delete ver;
    }

    // Clean up retired versions and their delete chains
    for (auto& retired : this->retired) {
        if (retired.data) {
            delete retired.data;
        }
        DeleteIndexBlock* blk = retired.blocksToDelete;
        while (blk) {
            DeleteIndexBlock* next = blk->next.load(std::memory_order_acquire);
            delete blk;
            blk = next;
        }
    }
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::deleteTileRecord(uint16_t rowId, uint64_t ts) {
    uint64_t item = makeDeleteIndex(rowId, ts);
    while (true) {
        DeleteIndexBlock *curTail = tail.load(std::memory_order_acquire);
        if (!curTail) { // empty list - need to create first block and update version
            auto *newBlk = new DeleteIndexBlock();
            newBlk->items[0] = item;
            DeleteIndexBlock *expectedTail = nullptr;

            if (!tail.compare_exchange_strong(expectedTail, newBlk,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                delete newBlk;
                continue;
            }

            // COW: Create new version with the new head
            VersionedData<CAPACITY>* oldVer = currentVersion.load(std::memory_order_acquire);
            VersionedData<CAPACITY>* newVer = new VersionedData<CAPACITY>(oldVer->baseTimestamp, oldVer->baseBitmap, newBlk);

            if (currentVersion.compare_exchange_strong(oldVer, newVer, std::memory_order_acq_rel)) {
                // Success: retire old version (no chain to delete since head was nullptr)
                delete oldVer;
                tailUsed.store(1, std::memory_order_release);
                return;
            } else {
                // CAS failed, retry from beginning
                delete newVer;
                tail.store(nullptr, std::memory_order_release);
                delete newBlk;
                continue;
            }
        } else {
            size_t pos = tailUsed.load(std::memory_order_acquire);
            if (pos < DeleteIndexBlock::BLOCK_CAPACITY) {
                if (tailUsed.compare_exchange_strong(pos, pos + 1,
                                                     std::memory_order_relaxed,
                                                     std::memory_order_relaxed)) {
                    curTail->items[pos] = item;
                    return;
                }
            } else {
                // curTail is full, need to add new block
                DeleteIndexBlock *newBlk = new DeleteIndexBlock();
                newBlk->items[0] = item;

                if (tail.load(std::memory_order_acquire) != curTail) {
                    delete newBlk;
                    continue;
                }

                DeleteIndexBlock *expectedNext = nullptr;
                if (!curTail->next.compare_exchange_strong(
                        expectedNext, newBlk,
                        std::memory_order_release,
                        std::memory_order_relaxed)) {
                    delete newBlk;
                    continue;
                }

                tail.compare_exchange_strong(curTail, newBlk,
                                             std::memory_order_release,
                                             std::memory_order_relaxed);
                tailUsed.store(1, std::memory_order_release);
                return;
            }
        }
    }
}

inline void process_bitmap_block_256(const DeleteIndexBlock *blk,
                                     uint32_t offset,
                                     uint64_t* outBitmap,
                                     const __m256i &vThrFlip,
                                     const __m256i &tsMask,
                                     const __m256i &signBit) {
    __m256i vItems = _mm256_loadu_si256((const __m256i *)&blk->items[offset]);
    __m256i vTs = _mm256_and_si256(vItems, tsMask);
    __m256i vTsFlip = _mm256_xor_si256(vTs, signBit);

    __m256i cmp = _mm256_or_si256(
        _mm256_cmpgt_epi64(vThrFlip, vTsFlip),
        _mm256_cmpeq_epi64(vThrFlip, vTsFlip)
        );

    uint8_t mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp));
    if (!mask) return;

    __m256i vRow = _mm256_srli_epi64(vItems, 48); // Fixed to 48 for larger RowID
    alignas(32) uint64_t rowTmp[4];
    _mm256_store_si256((__m256i *)rowTmp, vRow);
    for (int i = 0; i < 4; i++) {
        if (mask & (1 << i)) {
            SET_BITMAP_BIT(outBitmap, static_cast<uint16_t>(rowTmp[i]));
        }
    }
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::getTileVisibilityBitmap(uint64_t ts, uint64_t* outBitmap) const {
    // Enter epoch protection
    EpochGuard guard;
    // Load current version under epoch protection
    VersionedData<CAPACITY>* ver = currentVersion.load(std::memory_order_acquire);

    if (ts < ver->baseTimestamp) {
        throw std::runtime_error("need to read checkpoint from disk");
    }
    std::memcpy(outBitmap, ver->baseBitmap, NUM_WORDS * sizeof(uint64_t));
    if (ts == ver->baseTimestamp) return;

    DeleteIndexBlock *blk = ver->head;
#ifdef RETINA_SIMD
    const __m256i signBit = _mm256_set1_epi64x(0x8000000000000000ULL);
    const __m256i vThrFlip = _mm256_xor_si256(_mm256_set1_epi64x(ts), signBit);
    const __m256i tsMask = _mm256_set1_epi64x(0x0000FFFFFFFFFFFFULL);
#endif

    while (blk) {
        DeleteIndexBlock *currentTail = tail.load(std::memory_order_relaxed);
        size_t currentTailUsed = tailUsed.load(std::memory_order_relaxed);
        size_t count = (blk == currentTail) ? currentTailUsed : DeleteIndexBlock::BLOCK_CAPACITY;

        // Same tail/tailUsed race as in collectTileGarbage: count may be 0 or
        // a stale BLOCK_CAPACITY for a newly-created tail block.  count == 0
        // means no items to read; skip cleanly.  The stale-count case (items
        // beyond the first being zero-initialised) is handled in the scalar
        // path below via the item == 0 sentinel check.
        if (count == 0) {
            blk = blk->next.load(std::memory_order_relaxed);
            continue;
        }

        uint64_t i = 0;
#ifdef RETINA_SIMD
        // NOTE: the SIMD path does not check for zero-initialised (item == 0)
        // sentinel values.  In the extremely rare stale-tailUsed race window,
        // up to BLOCK_CAPACITY-1 zero items may cause row 0 to be transiently
        // marked as deleted in the output bitmap.  This is a known limitation
        // of the SIMD fast path; the effect is transient (not persisted) and
        // self-correcting on the next query once tailUsed is fully updated.
        for (; i + 4 <= count; i += 4) {
            process_bitmap_block_256(blk, i, outBitmap, vThrFlip, tsMask, signBit);
        }
#endif
        for (; i < count; i++) {
            uint64_t item = blk->items[i];
            // Sentinel: zero item signals an uninitialised slot (see
            // collectTileGarbage for the full race description).
            if (item == 0) return;
            if (extractTimestamp(item) <= ts) {
                SET_BITMAP_BIT(outBitmap, extractRowId(item));
            } else {
                return;
            }
        }
        blk = blk->next.load(std::memory_order_relaxed);
    }
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::collectTileGarbage(uint64_t ts) {
    // Load old version
    VersionedData<CAPACITY>* oldVer = currentVersion.load(std::memory_order_acquire);
    if (ts <= oldVer->baseTimestamp) return;

    // Find the last block that should be compacted
    DeleteIndexBlock *blk = oldVer->head;
    DeleteIndexBlock *lastFullBlk = nullptr;
    uint64_t newBaseTimestamp = oldVer->baseTimestamp;

    while (blk) {
        size_t count = (blk == tail.load(std::memory_order_acquire))
                           ? tailUsed.load(std::memory_order_acquire)
                           : DeleteIndexBlock::BLOCK_CAPACITY;
        // Guard: deleteTileRecord updates `tail` and `tailUsed` non-atomically.
        // In the narrow window after `tail` is advanced to a new block but before
        // `tailUsed.store(1)` completes, we may observe count == 0 (empty-list
        // path: tailUsed transitions 0 → 1) or a stale BLOCK_CAPACITY (full-block
        // path: tailUsed transitions BLOCK_CAPACITY → 1 via store, not CAS).
        // When count == 0 there is nothing to compact; stop here and let the next
        // GC cycle handle the block once it is fully initialised.
        if (count == 0) break;
        uint64_t lastItemTs = extractTimestamp(blk->items[count - 1]);
        if (lastItemTs <= ts) {
            lastFullBlk = blk;
            newBaseTimestamp = lastItemTs;
        } else break;
        blk = blk->next.load(std::memory_order_acquire);
    }

    if (!lastFullBlk) return;

    // Create new version with Copy-on-Write
    // Manually compute the new base bitmap from oldVer
    uint64_t newBaseBitmap[NUM_WORDS];
    std::memcpy(newBaseBitmap, oldVer->baseBitmap, NUM_WORDS * sizeof(uint64_t));

    // Apply deletes from oldVer->head up to lastFullBlk
    blk = oldVer->head;
    while (blk) {
        size_t count = (blk == lastFullBlk && blk == tail.load()) ? tailUsed.load() : DeleteIndexBlock::BLOCK_CAPACITY;
        for (size_t i = 0; i < count; i++) {
            uint64_t item = blk->items[i];
            // Guard: a zero item means an uninitialised slot in a newly-created
            // tail block observed under the same tail/tailUsed race described
            // above (full-block path: tailUsed is still BLOCK_CAPACITY while
            // only items[0] is valid; items[1..n] remain zero-initialised).
            // item == 0 encodes makeDeleteIndex(rowId=0, ts=0); since all valid
            // transaction timestamps are > 0, this value is never a legitimate
            // deletion record and safely identifies the end of valid items.
            if (item == 0) break;
            if (extractTimestamp(item) <= ts) {
                SET_BITMAP_BIT(newBaseBitmap, extractRowId(item));
            }
        }
        if (blk == lastFullBlk) break;
        blk = blk->next.load(std::memory_order_acquire);
    }

    // Get new head and break the chain to avoid double-free
    DeleteIndexBlock* newHead = lastFullBlk->next.load(std::memory_order_acquire);
    lastFullBlk->next.store(nullptr, std::memory_order_release);

    // Create new version with new head - this is the atomic COW update
    VersionedData<CAPACITY>* newVer = new VersionedData<CAPACITY>(newBaseTimestamp, newBaseBitmap, newHead);

    // CAS to install new version atomically
    if (currentVersion.compare_exchange_strong(oldVer, newVer, std::memory_order_acq_rel)) {
        // Successfully updated
        // Retire old version and its delete chain
        uint64_t retireEpoch = EpochManager::getInstance().advanceEpoch();
        retired.emplace_back(oldVer, oldVer->head, retireEpoch);

        // Update tail if needed (if all blocks were compacted)
        if (!newHead) {
            tail.store(nullptr, std::memory_order_release);
            tailUsed.store(0, std::memory_order_release);
        }

        // Try to reclaim retired versions
        reclaimRetiredVersions();
    } else {
        // CAS failed, another GC happened concurrently
        // Restore the chain link
        lastFullBlk->next.store(newHead, std::memory_order_release);
        delete newVer;
    }
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::reclaimRetiredVersions() {
    auto it = retired.begin();
    while (it != retired.end()) {
        if (EpochManager::getInstance().canReclaim(it->retireEpoch)) {
            // Safe to delete
            if (it->data) {
                delete it->data;
            }
            // Delete the chain of blocks
            DeleteIndexBlock* blk = it->blocksToDelete;
            while (blk) {
                DeleteIndexBlock* next = blk->next.load(std::memory_order_acquire);
                delete blk;
                blk = next;
            }
            it = retired.erase(it);
        } else {
            ++it;
        }
    }
}

// Explicit Instantiations (Add the sizes you need here)
template class TileVisibility<RETINA_CAPACITY>;
