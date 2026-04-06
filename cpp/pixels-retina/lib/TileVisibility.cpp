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

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include <immintrin.h>

template<size_t CAPACITY>
TileVisibility<CAPACITY>::TileVisibility(uint64_t timestamp, const uint64_t* bitmap)
    : currentVersion(new VersionedData<CAPACITY>(timestamp, bitmap)),
      tail(nullptr), tailUsed(0) {}

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

    // Clean up any version left in the pending retirement slot
    VersionedData<CAPACITY>* pending = pendingRetire.load(std::memory_order_acquire);
    if (pending) {
        delete pending;
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
                // Defer retirement: a concurrent reader may still hold oldVer under EpochGuard.
                // collectTileGarbage will drain this slot and epoch-retire it properly.
                pendingRetire.store(oldVer, std::memory_order_release);
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
void TileVisibility<CAPACITY>::collectTileGarbage(uint64_t ts, uint64_t* gcSnapshotBitmap) {
    // Drain the pending retirement slot left by deleteTileRecord's empty-chain path.
    VersionedData<CAPACITY>* pending = pendingRetire.exchange(nullptr, std::memory_order_acquire);
    if (pending) {
        uint64_t retireEpoch = EpochManager::getInstance().advanceEpoch();
        retired.emplace_back(pending, nullptr, retireEpoch);
    }

    // Load old version
    VersionedData<CAPACITY>* oldVer = currentVersion.load(std::memory_order_acquire);

    // Early return A: safeGcTs <= baseTimestamp, nothing to compact
    if (ts <= oldVer->baseTimestamp) {
        std::memcpy(gcSnapshotBitmap, oldVer->baseBitmap, NUM_WORDS * sizeof(uint64_t));
        return;
    }

    // Find the last block that should be compacted.
    // Snapshot tail/tailUsed once and reuse in both the scan loop and the
    // compact loop to guarantee a consistent view of the chain endpoint.
    DeleteIndexBlock *blk = oldVer->head;
    DeleteIndexBlock *lastFullBlk = nullptr;
    uint64_t newBaseTimestamp = oldVer->baseTimestamp;
    auto* tailSnap1 = tail.load(std::memory_order_acquire);
    size_t tailUsedSnap1 = tailUsed.load(std::memory_order_acquire);

    while (blk) {
        size_t count = (blk == tailSnap1)
                           ? tailUsedSnap1
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

    // Early return B: no compactable block
    if (!lastFullBlk) {
        std::memcpy(gcSnapshotBitmap, oldVer->baseBitmap, NUM_WORDS * sizeof(uint64_t));
        if (oldVer->head) {
            auto* tailSnap = tail.load(std::memory_order_acquire);
            size_t tailUsedSnap = tailUsed.load(std::memory_order_acquire);
            size_t cnt = (oldVer->head == tailSnap) ? tailUsedSnap : DeleteIndexBlock::BLOCK_CAPACITY;
            for (size_t i = 0; i < cnt; i++) {
                uint64_t item = oldVer->head->items[i];
                if (item == 0) break;
                if (extractTimestamp(item) <= ts) SET_BITMAP_BIT(gcSnapshotBitmap, extractRowId(item));
                else break;
            }
        }
        return;
    }

    // Create new version with Copy-on-Write
    // Manually compute the new base bitmap from oldVer
    uint64_t newBaseBitmap[NUM_WORDS];
    std::memcpy(newBaseBitmap, oldVer->baseBitmap, NUM_WORDS * sizeof(uint64_t));

    // Apply deletes from oldVer->head up to lastFullBlk
    blk = oldVer->head;
    while (blk) {
        size_t count = (blk == lastFullBlk && blk == tailSnap1) ? tailUsedSnap1 : DeleteIndexBlock::BLOCK_CAPACITY;
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

    // Compact path: build gcSnapshotBitmap by scanning the boundary block.
    // Reuse the same tail/tailUsed snapshot (tailSnap1/tailUsedSnap1) taken at
    // the start of this GC cycle to ensure consistent chain-end semantics.
    DeleteIndexBlock* newHead = lastFullBlk->next.load(std::memory_order_acquire);
    std::memcpy(gcSnapshotBitmap, newBaseBitmap, NUM_WORDS * sizeof(uint64_t));
    if (newHead) {
        size_t cnt = (newHead == tailSnap1) ? tailUsedSnap1 : DeleteIndexBlock::BLOCK_CAPACITY;
        for (size_t i = 0; i < cnt; i++) {
            uint64_t item = newHead->items[i];
            if (item == 0) break;
            if (extractTimestamp(item) <= ts) SET_BITMAP_BIT(gcSnapshotBitmap, extractRowId(item));
            else break;
        }
    }

    // Break the chain to avoid double-free
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

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::exportChainItemsAfter(
    uint32_t tileId, uint64_t safeGcTs,
    std::vector<std::pair<uint32_t, uint64_t>>& gcChainItems) const {
    auto* ver = currentVersion.load(std::memory_order_acquire);
    auto* tailSnap = tail.load(std::memory_order_acquire);
    size_t tailUsedSnap = tailUsed.load(std::memory_order_acquire);

    auto* blk = ver->head;
    bool pastBoundary = false;
    while (blk != nullptr) {
        size_t count = (blk == tailSnap) ? tailUsedSnap : DeleteIndexBlock::BLOCK_CAPACITY;
        for (size_t i = 0; i < count; i++) {
            uint64_t item = blk->items[i];
            if (item == 0) return;
            if (pastBoundary) {
                uint32_t rgOffset = tileId * CAPACITY + extractRowId(item);
                gcChainItems.push_back({rgOffset, extractTimestamp(item)});
            } else {
                uint64_t ts = extractTimestamp(item);
                if (ts > safeGcTs) {
                    pastBoundary = true;
                    uint32_t rgOffset = tileId * CAPACITY + extractRowId(item);
                    gcChainItems.push_back({rgOffset, ts});
                }
            }
        }
        if (blk == tailSnap) return;
        blk = blk->next.load(std::memory_order_acquire);
    }
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::importDeletionItems(std::vector<uint64_t>& bucket) {
    std::sort(bucket.begin(), bucket.end(), [](uint64_t a, uint64_t b) {
        return extractTimestamp(a) < extractTimestamp(b);
    });

    bool tailClaimed = false;
    while (true) {
        auto* ver = currentVersion.load(std::memory_order_acquire);

        uint64_t ts_head = UINT64_MAX;
        if (ver->head != nullptr) {
            uint64_t firstItem = ver->head->items[0];
            if (firstItem != 0) ts_head = extractTimestamp(firstItem);
        }

        size_t keepCount = bucket.size();
        if (ts_head != UINT64_MAX) {
            keepCount = std::upper_bound(bucket.begin(), bucket.end(), ts_head,
                [](uint64_t val, uint64_t item) {
                    return val < extractTimestamp(item);
                }) - bucket.begin();
        }
        if (keepCount == 0) return;

        uint64_t lastValidItem = bucket[keepCount - 1];
        std::vector<DeleteIndexBlock*> blocks;
        for (size_t i = 0; i < keepCount; i += DeleteIndexBlock::BLOCK_CAPACITY) {
            auto* blk = new DeleteIndexBlock();
            for (size_t j = 0; j < DeleteIndexBlock::BLOCK_CAPACITY; j++) {
                size_t idx = i + j;
                blk->items[j] = (idx < keepCount) ? bucket[idx] : lastValidItem;
            }
            blocks.push_back(blk);
        }
        for (size_t i = 0; i + 1 < blocks.size(); i++)
            blocks[i]->next.store(blocks[i + 1], std::memory_order_release);
        blocks.back()->next.store(ver->head, std::memory_order_release);

        if (ver->head == nullptr && !tailClaimed) {
            size_t lastBlockItems = keepCount % DeleteIndexBlock::BLOCK_CAPACITY;
            if (lastBlockItems == 0) lastBlockItems = DeleteIndexBlock::BLOCK_CAPACITY;

            DeleteIndexBlock* expectedTail = nullptr;
            if (tail.compare_exchange_strong(expectedTail, blocks.back(),
                                             std::memory_order_release, std::memory_order_relaxed)) {
                tailUsed.store(lastBlockItems, std::memory_order_release);
                tailClaimed = true;
            } else {
                for (auto* blk : blocks) delete blk;
                continue;
            }
        }

        auto* newVer = new VersionedData<CAPACITY>(ver->baseTimestamp, ver->baseBitmap, blocks[0]);

        if (currentVersion.compare_exchange_strong(ver, newVer, std::memory_order_acq_rel)) {
            uint64_t retireEpoch = EpochManager::getInstance().advanceEpoch();
            retired.emplace_back(ver, nullptr, retireEpoch);
            reclaimRetiredVersions();
            return;
        }
        if (tailClaimed) {
            std::fprintf(stderr, "importDeletionItems: CAS failed with tailClaimed — invariant violation\n");
            std::abort();
        }
        delete newVer;
        for (auto* blk : blocks) delete blk;
    }
}

// Explicit Instantiations (Add the sizes you need here)
template class TileVisibility<RETINA_CAPACITY>;