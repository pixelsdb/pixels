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

constexpr size_t DeleteIndexBlock::BLOCK_CAPACITY;

#if defined(RETINA_SIMD) && (defined(__x86_64__) || defined(__i386__))
#include <immintrin.h>
#define RETINA_X86_AVX2 1
#endif
#if defined(RETINA_SIMD) && (defined(__aarch64__) || defined(_M_ARM64))
#include <arm_neon.h>
#define RETINA_ARM_NEON 1
#endif

template<size_t CAPACITY>
TileVisibility<CAPACITY>::TileVisibility(uint64_t timestamp, const uint64_t* bitmap)
    : currentVersion(new VersionedData<CAPACITY>(timestamp, bitmap)) {}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::deleteChain(DeleteIndexBlock* blk) {
    while (blk) {
        auto* next = blk->next;
        delete blk;
        blk = next;
    }
}

template<size_t CAPACITY>
TileVisibility<CAPACITY>::~TileVisibility() {
    // Destruction requires callers to have stopped using this tile.
    auto* current = currentVersion.load(std::memory_order_relaxed);
    deleteChain(current->head);
    delete current;
    auto* pending = pendingRetire.exchange(nullptr);
    while (pending) {
        auto* next = pending->next;
        retired.push_back(pending);
        pending = next;
    }
    for (auto* entry : retired) {
        if (entry->wholeChain) deleteChain(entry->blocksToDelete);
        else delete entry->blocksToDelete;
        delete entry->data;
        delete entry;
    }
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::retireVersion(VersionedData<CAPACITY>* version,
                                            DeleteIndexBlock* blocks, bool wholeChain) {
    auto epoch = EpochManager::getInstance().advanceEpoch();
    auto* entry = new RetiredVersion<CAPACITY>(version, blocks, wholeChain, epoch);
    auto* pending = pendingRetire.load(std::memory_order_relaxed);
    do {
        entry->next = pending;
    } while (!pendingRetire.compare_exchange_weak(pending, entry,
                std::memory_order_release, std::memory_order_relaxed));
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::deleteTileRecord(uint16_t rowId, uint64_t ts, ReplayMode mode) {
    {
        EpochGuard guard;
        switch (mode) {
            case ReplayMode::NORMAL: appendDeleteChain(rowId, ts); break;
            case ReplayMode::VERSIONED: deleteTileRecordVersioned(rowId, ts); break;
            case ReplayMode::EXCLUSIVE: deleteTileRecordExclusive(rowId, ts); break;
            default: throw std::invalid_argument("unknown ReplayMode");
        }
    }
    // Bound obsolete publication states between GC cycles. Reclamation runs
    // outside our epoch and does not block a concurrent publisher or reader.
    if ((retireCount.fetch_add(1, std::memory_order_relaxed) & 63) == 0)
        reclaimRetiredVersions();
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::deleteTileRecordVersioned(uint16_t rowId, uint64_t ts) {
    while (ts > 0) {
        auto* cur = currentVersion.load(std::memory_order_acquire);
        if (ts > cur->baseTimestamp) break;
        if (cur->baseBitmap[rowId / 64] & (1ULL << (rowId % 64))) return;
        uint64_t bitmap[NUM_WORDS];
        std::memcpy(bitmap, cur->baseBitmap, sizeof(bitmap));
        SET_BITMAP_BIT(bitmap, rowId);
        auto* replacement = new VersionedData<CAPACITY>(cur->baseTimestamp, bitmap, cur->head);
        if (currentVersion.compare_exchange_strong(cur, replacement, std::memory_order_acq_rel)) {
            retireVersion(cur, nullptr, false);
            return;
        }
        delete replacement;
    }
    appendDeleteChain(rowId, ts);
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::deleteTileRecordExclusive(uint16_t rowId, uint64_t ts) {
    // Recovery writers also use immutable bitmap publication. In-place atomic
    // OR would race with another writer copying the bitmap for a new state.
    deleteTileRecordVersioned(rowId, ts);
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::appendDeleteChain(uint16_t rowId, uint64_t ts) {
    uint64_t item = makeDeleteIndex(rowId, ts);
    // Preserve the existing reserved-zero encoding; transaction timestamps are
    // nonzero, and (row 0, timestamp 0) is not a deletion record.
    if (item == 0) return;
    while (true) {
        auto* cur = currentVersion.load(std::memory_order_acquire);
        auto* head = new DeleteIndexBlock();
        bool copyHead = cur->head && cur->head->count < DeleteIndexBlock::BLOCK_CAPACITY;
        if (copyHead) {
            head->count = cur->head->count;
            std::memcpy(head->items, cur->head->items, head->count * sizeof(uint64_t));
            head->next = cur->head->next;
        } else head->next = cur->head;
        head->items[head->count++] = item;
        auto* replacement = new VersionedData<CAPACITY>(*cur, head);
        if (currentVersion.compare_exchange_strong(cur, replacement, std::memory_order_acq_rel)) {
            retireVersion(cur, copyHead ? cur->head : nullptr, false);
            return;
        }
        delete replacement;
        delete head;
    }
}

#if defined(RETINA_X86_AVX2)
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
#endif

#if defined(RETINA_ARM_NEON)
inline void process_bitmap_block_128(const DeleteIndexBlock *blk,
                                     uint32_t offset,
                                     uint64_t* outBitmap,
                                     const uint64x2_t &vThr,
                                     const uint64x2_t &tsMask) {
    uint64x2_t vItems = vld1q_u64(&blk->items[offset]);
    uint64x2_t vTs = vandq_u64(vItems, tsMask);
    uint64x2_t cmp = vcgeq_u64(vThr, vTs);

    uint64x2_t vRow = vshrq_n_u64(vItems, 48);
    uint64_t row0 = vgetq_lane_u64(vRow, 0);
    uint64_t row1 = vgetq_lane_u64(vRow, 1);
    if (vgetq_lane_u64(cmp, 0)) {
        SET_BITMAP_BIT(outBitmap, static_cast<uint16_t>(row0));
    }
    if (vgetq_lane_u64(cmp, 1)) {
        SET_BITMAP_BIT(outBitmap, static_cast<uint16_t>(row1));
    }
}
#endif

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::getTileVisibilityBitmap(uint64_t ts, uint64_t* bitmap) const {
    EpochGuard guard;
    auto* ver = currentVersion.load(std::memory_order_acquire);
    if (ts < ver->baseTimestamp) throw std::runtime_error("need to read checkpoint from disk");
    std::memcpy(bitmap, ver->baseBitmap, NUM_WORDS * sizeof(uint64_t));
    if (ts == ver->baseTimestamp) return;
#if defined(RETINA_X86_AVX2)
    const __m256i signBit = _mm256_set1_epi64x(0x8000000000000000ULL);
    const __m256i vThrFlip = _mm256_xor_si256(_mm256_set1_epi64x(ts), signBit);
    const __m256i tsMask = _mm256_set1_epi64x(0x0000FFFFFFFFFFFFULL);
#elif defined(RETINA_ARM_NEON)
    const uint64x2_t vThr = vdupq_n_u64(ts);
    const uint64x2_t tsMask = vdupq_n_u64(0x0000FFFFFFFFFFFFULL);
#endif
    for (auto* blk = ver->head; blk; blk = blk->next) {
        size_t i = 0;
#if defined(RETINA_X86_AVX2)
        for (; i + 4 <= blk->count; i += 4)
            process_bitmap_block_256(blk, i, bitmap, vThrFlip, tsMask, signBit);
#elif defined(RETINA_ARM_NEON)
        for (; i + 2 <= blk->count; i += 2)
            process_bitmap_block_128(blk, i, bitmap, vThr, tsMask);
#endif
        for (; i < blk->count; ++i) {
            auto item = blk->items[i];
            if (extractTimestamp(item) <= ts) SET_BITMAP_BIT(bitmap, extractRowId(item));
        }
    }
}

template<size_t CAPACITY>
DeleteIndexBlock* TileVisibility<CAPACITY>::buildChain(const std::vector<uint64_t>& items,
                                                      DeleteIndexBlock* next) {
    auto* head = next;
    // Each block is completely initialized before it becomes reachable.
    for (size_t offset = 0; offset < items.size(); offset += DeleteIndexBlock::BLOCK_CAPACITY) {
        auto* block = new DeleteIndexBlock();
        block->count = std::min(DeleteIndexBlock::BLOCK_CAPACITY, items.size() - offset);
        std::memcpy(block->items, items.data() + offset, block->count * sizeof(uint64_t));
        block->next = head;
        head = block;
    }
    return head;
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::collectTileGarbage(uint64_t ts, uint64_t* snapshot) {
    {
        EpochGuard guard;
        while (true) {
            auto* old = currentVersion.load(std::memory_order_acquire);
            std::memcpy(snapshot, old->baseBitmap, NUM_WORDS * sizeof(uint64_t));
            if (ts <= old->baseTimestamp) break;
            uint64_t baseTs = old->baseTimestamp;
            std::vector<uint64_t> future;
            bool folded = false;
            for (auto* block = old->head; block; block = block->next) {
                for (size_t i = 0; i < block->count; ++i) {
                    auto item = block->items[i];
                    auto itemTs = extractTimestamp(item);
                    if (itemTs <= ts) {
                        SET_BITMAP_BIT(snapshot, extractRowId(item));
                        baseTs = std::max(baseTs, itemTs);
                        folded = true;
                    } else future.push_back(item);
                }
            }
            if (!folded) break;
            auto* head = buildChain(future);
            auto* replacement = new VersionedData<CAPACITY>(baseTs, snapshot, head);
            if (currentVersion.compare_exchange_strong(old, replacement, std::memory_order_acq_rel)) {
                // No published links were changed. Readers of old keep their
                // full chain, and future deletes remain in the replacement.
                retireVersion(old, old->head, true);
                break;
            }
            delete replacement;
            deleteChain(head);
        }
    }
    reclaimRetiredVersions();
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::reclaimRetiredVersions() {
    if (reclaiming.test_and_set(std::memory_order_acquire)) return;
    auto* pending = pendingRetire.exchange(nullptr, std::memory_order_acquire);
    while (pending) {
        auto* next = pending->next;
        retired.push_back(pending);
        pending = next;
    }
    // Reclaim old entries even while other writers continue to publish. Waiting
    // for the newest entry would require a global quiescent gap under load.
    const auto boundary = EpochManager::getInstance().oldestActiveEpoch();
    size_t keep = 0;
    for (auto* entry : retired) {
        if (entry->retireEpoch < boundary) {
            if (entry->wholeChain) deleteChain(entry->blocksToDelete);
            else delete entry->blocksToDelete;
            delete entry->data;
            delete entry;
        } else retired[keep++] = entry;
    }
    retired.resize(keep);
    reclaiming.clear(std::memory_order_release);
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::exportChainItemsAfter(uint32_t tileId, uint64_t safeGcTs,
    std::vector<std::pair<uint32_t, uint64_t>>& output) const {
    EpochGuard guard;
    auto* ver = currentVersion.load(std::memory_order_acquire);
    const size_t start = output.size();
    for (auto* block = ver->head; block; block = block->next) {
        for (size_t i = 0; i < block->count; ++i) {
            auto item = block->items[i];
            auto ts = extractTimestamp(item);
            if (ts > safeGcTs) output.emplace_back(tileId * CAPACITY + extractRowId(item), ts);
        }
    }
    // Preserve the checkpoint API's chronological output without requiring CDC
    // threads to append in timestamp order.
    std::stable_sort(output.begin() + start, output.end(), [](const std::pair<uint32_t, uint64_t>& a,
                                                           const std::pair<uint32_t, uint64_t>& b) {
        return a.second < b.second;
    });
}

template<size_t CAPACITY>
void TileVisibility<CAPACITY>::importDeletionItems(std::vector<uint64_t>& bucket) {
    if (bucket.empty()) return;
    {
        EpochGuard guard;
        while (true) {
            auto* old = currentVersion.load(std::memory_order_acquire);
            uint64_t bitmap[NUM_WORDS];
            std::memcpy(bitmap, old->baseBitmap, sizeof(bitmap));
            std::vector<uint64_t> future;
            for (auto item : bucket) {
                if (extractTimestamp(item) <= old->baseTimestamp)
                    SET_BITMAP_BIT(bitmap, extractRowId(item));
                else future.push_back(item);
            }
            // Different rows at the same/later timestamp are not duplicates.
            // Replaying a duplicate is harmless for the deletion bitmap.
            auto* oldHead = old->head;
            auto* head = buildChain(future, oldHead);
            auto* replacement = new VersionedData<CAPACITY>(old->baseTimestamp, bitmap, head);
            if (currentVersion.compare_exchange_strong(old, replacement, std::memory_order_acq_rel)) {
                retireVersion(old, nullptr, false);
                break;
            }
            delete replacement;
            // Only our unpublished prefix belongs to this failed attempt.
            while (head != oldHead) {
                auto* next = head->next;
                delete head;
                head = next;
            }
        }
    }
    reclaimRetiredVersions();
}

template class TileVisibility<RETINA_CAPACITY>;
