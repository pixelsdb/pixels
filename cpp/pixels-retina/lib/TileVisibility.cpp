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

#include "RetinaMemory.h"
#include "TileVisibility.h"
#include "EpochManager.h"

#include <cstring>
#include <stdexcept>

#include <immintrin.h>

TileVisibility::TileVisibility() {
    VersionedData* initialVersion = new VersionedData();
    currentVersion.store(initialVersion, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
    tailUsed.store(0, std::memory_order_release);
}

TileVisibility::TileVisibility(uint64_t ts, const uint64_t bitmap[4]) {
    VersionedData* initialVersion = new VersionedData(ts, bitmap, nullptr);
    currentVersion.store(initialVersion, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
    tailUsed.store(0, std::memory_order_release);
}

TileVisibility::~TileVisibility() {
    // Clean up current version and its delete chain
    VersionedData* ver = currentVersion.load(std::memory_order_acquire);
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

void TileVisibility::deleteTileRecord(uint8_t rowId, uint64_t ts) {
    uint64_t item = makeDeleteIndex(rowId, ts);
    while (true) {
        DeleteIndexBlock *curTail = tail.load(std::memory_order_acquire);
        if (!curTail) { // empty list - need to create first block and update version
            DeleteIndexBlock *newBlk = new DeleteIndexBlock();
            newBlk->items[0] = item;
            DeleteIndexBlock *expectedTail = nullptr;
            
            if (!tail.compare_exchange_strong(expectedTail, newBlk,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                delete newBlk;
                continue;
            }
            
            // COW: Create new version with the new head
            VersionedData* oldVer = currentVersion.load(std::memory_order_acquire);
            VersionedData* newVer = new VersionedData(oldVer->baseTimestamp, oldVer->baseBitmap, newBlk);
            
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
                                     uint64_t outBitmap[4],
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

    if (!mask)
        return;

    __m256i vRow = _mm256_srli_epi64(vItems, 56); // extract rowid
    alignas(32) uint64_t rowTmp[8];
    _mm256_storeu_si256((__m256i *)rowTmp, vRow);
    for (int i = 0; i < 4; i++) {
        if (mask & (1 << i)) {
            auto rowId = static_cast<uint8_t>(rowTmp[i]);
            SET_BITMAP_BIT(outBitmap, rowId);
        }
    }
}

void TileVisibility::getTileVisibilityBitmap(uint64_t ts, uint64_t outBitmap[4]) const {
    // Enter epoch protection
    EpochGuard guard;
    
    // Load current version under epoch protection
    VersionedData* ver = currentVersion.load(std::memory_order_acquire);
    
    if (ts < ver->baseTimestamp) {
        throw std::runtime_error("need to read checkpoint from disk");
    }
    std::memcpy(outBitmap, ver->baseBitmap, 4 * sizeof(uint64_t));
    if (ts == ver->baseTimestamp) {
        return;
    }

    DeleteIndexBlock *blk = ver->head;
#if defined(RETINA_SIMD) && !defined(DISABLE_RETINA_SIMD)
    const __m256i signBit = _mm256_set1_epi64x(0x8000000000000000ULL);
    const __m256i vThrFlip = _mm256_xor_si256(_mm256_set1_epi64x(ts), signBit);
    const __m256i tsMask = _mm256_set1_epi64x(0x00FFFFFFFFFFFFFFULL);
#endif

    while (blk) {
        DeleteIndexBlock *currentTail = tail.load(std::memory_order_relaxed);
        size_t currentTailUsed = tailUsed.load(std::memory_order_relaxed);
        size_t count = (blk == currentTail)
                           ? currentTailUsed
                           : DeleteIndexBlock::BLOCK_CAPACITY;
        if (count > DeleteIndexBlock::BLOCK_CAPACITY) {
            continue; // retry get count
        }
        uint64_t start_blk_offset = 0;
#if defined(RETINA_SIMD) && !defined(DISABLE_RETINA_SIMD)
        if (count == DeleteIndexBlock::BLOCK_CAPACITY) {
            process_bitmap_block_256(blk, 0, outBitmap, vThrFlip, tsMask, signBit);
            process_bitmap_block_256(blk, 4, outBitmap, vThrFlip, tsMask, signBit);
        } else if (count >= 4) {
            start_blk_offset = 4;
            process_bitmap_block_256(blk, 0, outBitmap, vThrFlip, tsMask, signBit);
        }
#endif
        for (uint64_t i = start_blk_offset; i < count; i++) {
            uint64_t item = blk->items[i];
            uint64_t delTs = extractTimestamp(item);
            if (delTs <= ts) {
                SET_BITMAP_BIT(outBitmap, extractRowId(item));
            } else {
                // delTs is increasing, so no need to check further
                return;
            }
        }

        if (blk == currentTail) {
            if (currentTail != tail.load(std::memory_order_acquire) ||
                currentTailUsed != tailUsed.load(std::memory_order_acquire)) {
                // no need to reset outBitmap, just lost the latest deletion
                continue;
            }
        }

        blk = blk->next.load(std::memory_order_relaxed);
    }
}

void TileVisibility::collectTileGarbage(uint64_t ts) {
    // Load old version
    VersionedData* oldVer = currentVersion.load(std::memory_order_acquire);
    
    if (ts <= oldVer->baseTimestamp) {
        return;
    }

    // Find the last block that should be compacted
    DeleteIndexBlock *blk = oldVer->head;
    DeleteIndexBlock *lastFullBlk = nullptr;
    uint64_t newBaseTimestamp = oldVer->baseTimestamp;

    while (blk) {
        size_t count = (blk == tail.load(std::memory_order_acquire))
                           ? tailUsed.load(std::memory_order_acquire)
                           : DeleteIndexBlock::BLOCK_CAPACITY;
        if (count > DeleteIndexBlock::BLOCK_CAPACITY) {
            throw std::runtime_error(
                "The number of item in block is bigger than BLOCK_CAPACITY");
        }

        uint64_t lastItemTs = extractTimestamp(blk->items[count - 1]);
        if (lastItemTs <= ts) {
            lastFullBlk = blk;
            newBaseTimestamp = lastItemTs;
        } else {
            break;
        }

        blk = blk->next.load(std::memory_order_acquire);
    }

    if (!lastFullBlk) {
        // Nothing to compact
        return;
    }

    // Create new version with Copy-on-Write
    // Manually compute the new base bitmap from oldVer
    uint64_t newBaseBitmap[4];
    std::memcpy(newBaseBitmap, oldVer->baseBitmap, 4 * sizeof(uint64_t));
    
    // Apply deletes from oldVer->head up to lastFullBlk
    blk = oldVer->head;
    while (blk) {
        size_t count = (blk == lastFullBlk) 
                           ? ((blk == tail.load(std::memory_order_acquire))
                               ? tailUsed.load(std::memory_order_acquire)
                               : DeleteIndexBlock::BLOCK_CAPACITY)
                           : DeleteIndexBlock::BLOCK_CAPACITY;
        
        for (size_t i = 0; i < count; i++) {
            uint64_t item = blk->items[i];
            uint64_t delTs = extractTimestamp(item);
            if (delTs <= ts) {
                SET_BITMAP_BIT(newBaseBitmap, extractRowId(item));
            }
        }
        
        if (blk == lastFullBlk) {
            break;
        }
        blk = blk->next.load(std::memory_order_acquire);
    }
    
    // Get new head and break the chain to avoid double-free
    DeleteIndexBlock* newHead = lastFullBlk->next.load(std::memory_order_acquire);
    lastFullBlk->next.store(nullptr, std::memory_order_release);
    
    // Create new version with new head - this is the atomic COW update
    VersionedData* newVer = new VersionedData(newBaseTimestamp, newBaseBitmap, newHead);

    // CAS to install new version atomically
    if (currentVersion.compare_exchange_strong(oldVer, newVer, 
                                               std::memory_order_acq_rel)) {
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

void TileVisibility::reclaimRetiredVersions() {
    // Remove retired versions that can be safely reclaimed
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
