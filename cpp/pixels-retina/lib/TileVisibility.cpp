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

#include <cstring>
#include <stdexcept>

#include <immintrin.h>

TileVisibility::TileVisibility() : baseTimestamp(0UL) {
    memset(baseBitmap, 0, 4 * sizeof(uint64_t));
    head.store(nullptr, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
    tailUsed.store(0, std::memory_order_release);
}

TileVisibility::TileVisibility(uint64_t ts, const uint64_t bitmap[4])
    : baseTimestamp(ts) {
    memcpy(baseBitmap, bitmap, 4 * sizeof(uint64_t));
    head.store(nullptr, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
}

TileVisibility::~TileVisibility() {
    DeleteIndexBlock *blk = head.load(std::memory_order_acquire);
    while (blk) {
        DeleteIndexBlock *next = blk->next.load(std::memory_order_acquire);
        delete blk;
        blk = next;
    }
}

void TileVisibility::deleteTileRecord(uint8_t rowId, uint64_t ts) {
    uint64_t item = makeDeleteIndex(rowId, ts);
    while (true) {
        DeleteIndexBlock *curTail = tail.load(std::memory_order_acquire);
        if (!curTail) { // empty list
            /**
             * Issue: There is a delay in reading.
             * Reads are judged from the head, and if the head pointer is
             * not changed in time, the latest data cannot be read.
             */
            DeleteIndexBlock *newBlk = new DeleteIndexBlock();
            newBlk->items[0] = item;
            DeleteIndexBlock *expectedTail = nullptr;
            
            if (!tail.compare_exchange_strong(expectedTail, newBlk,
                                              std::memory_order_acq_rel)) {
                delete newBlk;
                continue;
            }
            head.store(newBlk, std::memory_order_release);
            tailUsed.store(1, std::memory_order_release);
            return;
        } else {
            size_t pos = tailUsed.load(std::memory_order_acquire);
            if (pos < DeleteIndexBlock::BLOCK_CAPACITY) {
                if (tailUsed.compare_exchange_strong(pos, pos + 1, std::memory_order_acq_rel)) {
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
                        expectedNext, newBlk, std::memory_order_acq_rel)) {
                    delete newBlk;
                    continue;
                }

                tail.compare_exchange_strong(curTail, newBlk,
                                             std::memory_order_acq_rel);
                tailUsed.store(1, std::memory_order_release);
                return;
            }
        }
    }
}


inline void process_bitmap_block_256(const DeleteIndexBlock *blk,
    uint32_t offset,
    uint64_t outBitmap[4],
     const __m256i& vThr, const __m256i& tsMask) {
    __m256i vItems = _mm256_loadu_si256((const __m256i*)blk->items[offset]);
    __m256i vTs    = _mm256_and_si256(vItems, tsMask);
    __mmask8 mask    = _mm256_cmplt_epi64_mask(vTs, vThr);

    if (!mask) return;

    __m256i vRow = _mm256_srli_epi64(vItems, 56); // extract rowid
    alignas(32) uint64_t rowTmp[8];
    _mm256_storeu_si256((__m256i*)rowTmp, vRow);
    for (int i = 0; i < 4; i++) {
        if (mask & (1 << i)) {
            auto rowId = static_cast<uint8_t>(rowTmp[i]);
            SET_BITMAP_BIT(outBitmap, rowId);
        }
    }
}

void TileVisibility::getTileVisibilityBitmap(uint64_t ts, uint64_t outBitmap[4]) const {
    if (ts < baseTimestamp) {
        throw std::runtime_error("need to read checkpoint from disk");
    }
    std::memcpy(outBitmap, baseBitmap, 4 * sizeof(uint64_t));
    if (ts == baseTimestamp) {
        return;
    }

    DeleteIndexBlock *blk = head.load(std::memory_order_acquire);

    const __m256i vThr = _mm256_set1_epi64x(ts);
    const __m256i tsMask = _mm256_set1_epi64x(0x00FFFFFFFFFFFFFFULL);
    while (blk) {
        DeleteIndexBlock *currentTail = tail.load(std::memory_order_acquire);
        size_t currentTailUsed = tailUsed.load(std::memory_order_acquire);
        size_t count = (blk == currentTail) 
                        ? currentTailUsed
                        : DeleteIndexBlock::BLOCK_CAPACITY;
        if (count > DeleteIndexBlock::BLOCK_CAPACITY) {
            continue; // retry get count
        }
#define RETINA_SIMD
#ifdef RETINA_SIMD
        if (count == DeleteIndexBlock::BLOCK_CAPACITY) {
            process_bitmap_block_256(blk, 0, outBitmap, vThr, tsMask);
            process_bitmap_block_256(blk, 4, outBitmap, vThr, tsMask);
        } else {
#endif
            for (uint64_t i = 0; i < count; i++) {
                uint64_t item = blk->items[i];
                uint64_t delTs = extractTimestamp(item);
                if (delTs <= ts) {
                    SET_BITMAP_BIT(outBitmap, extractRowId(item));
                } else {
                    // delTs is increasing, so no need to check further
                    return;
                }
            }
#ifdef RETINA_SIMD
        }
#endif

        if (blk == currentTail) {
            if (currentTail != tail.load(std::memory_order_acquire) ||
                currentTailUsed != tailUsed.load(std::memory_order_acquire)) {
                // no need to reset outBitmap, just lost the latest deletion
                continue;
            }
        }

        blk = blk->next.load(std::memory_order_acquire);
    }
}

void TileVisibility::collectTileGarbage(uint64_t ts) {
    // The upper layers have ensured that there are no reads or writes at this point
    // so we can safely delete the records

    if (ts < baseTimestamp) {
        throw std::runtime_error("need to read checkpoint from disk");
    }

    if (ts == baseTimestamp) {
        return;
    }

    DeleteIndexBlock *blk = head.load(std::memory_order_acquire);
    DeleteIndexBlock *lastFullBlk = nullptr;
    uint64_t newBaseTimestamp = baseTimestamp;

    while (blk) {
        size_t count = (blk == tail.load(std::memory_order_acquire))
                           ? tailUsed.load(std::memory_order_acquire)
                           : DeleteIndexBlock::BLOCK_CAPACITY;
        if (count > DeleteIndexBlock::BLOCK_CAPACITY) {
            throw std::runtime_error(
                "The number of item in block is bigger than BLOCK_CAPCITY");
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

    if (lastFullBlk) {
        getTileVisibilityBitmap(ts, baseBitmap);
        baseTimestamp = newBaseTimestamp;

        DeleteIndexBlock *current = head.load(std::memory_order_acquire);
        DeleteIndexBlock *newHead =
            lastFullBlk->next.load(std::memory_order_acquire);

        head.store(newHead, std::memory_order_release);

        DeleteIndexBlock *curTail = tail.load(std::memory_order_acquire);
        if (!newHead) {
            tail.store(newHead, std::memory_order_release);
        }

        while (current != lastFullBlk->next.load(std::memory_order_acquire)) {
            DeleteIndexBlock *next = current->next.load(
                std::memory_order_acquire);
            delete current;
            current = next;
        }
    }
}
