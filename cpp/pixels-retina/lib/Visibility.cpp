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

#include "Visibility.h"

#include <cstring>
#include <stdexcept>

#ifndef SET_BITMAP_BIT
#define SET_BITMAP_BIT(bitmap, rowId)                                          \
    ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#endif

Visibility::Visibility() : baseTimestamp(0UL) {
    memset(baseBitmap, 0, 4 * sizeof(uint64_t));
    head.store(nullptr, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
}

Visibility::Visibility(uint64_t ts, const uint64_t bitmap[4])
    : baseTimestamp(ts) {
    memcpy(baseBitmap, bitmap, 4 * sizeof(uint64_t));
    head.store(nullptr, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
}

Visibility::~Visibility() {
    DeleteIndexBlock *blk = head.load(std::memory_order_acquire);
    while (blk) {
        DeleteIndexBlock *next = blk->next.load(std::memory_order_acquire);
        delete blk;
        blk = next;
    }
}

void Visibility::deleteRecord(uint8_t rowId, uint64_t ts) {
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
            newBlk->next.store(nullptr, std::memory_order_relaxed);
            newBlk->used.store(1, std::memory_order_release);
            DeleteIndexBlock *expectedTail = nullptr;
            if (!tail.compare_exchange_strong(expectedTail, newBlk,
                                              std::memory_order_acq_rel)) {
                delete newBlk;
                continue;
            }
            head.store(newBlk, std::memory_order_release);
            return;
        } else {
            uint8_t pos = curTail->used.fetch_add(1, std::memory_order_acquire);
            if (pos < 8) {
                curTail->items[pos] = item;
                return;
            } else {
                curTail->used.fetch_sub(1, std::memory_order_release);
                // curTail is full, need to add new block
                DeleteIndexBlock *newBlk = new DeleteIndexBlock();
                newBlk->items[0] = item;
                newBlk->next.store(nullptr, std::memory_order_relaxed);
                newBlk->used.store(1, std::memory_order_release);
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
                return;
            }
        }
    }
}

void Visibility::getVisibilityBitmap(uint64_t ts, uint64_t outBitmap[4]) const {
    if (ts < baseTimestamp) {
        throw std::runtime_error("need to read checkpoint from disk");
    }
    std::memcpy(outBitmap, baseBitmap, 4 * sizeof(uint64_t));
    if (ts == baseTimestamp) {
        return;
    }

    DeleteIndexBlock *blk = head.load(std::memory_order_acquire);
    while (blk) {
        uint8_t count = blk->used.load(std::memory_order_acquire);
        if (count > 8) {
            throw std::runtime_error("invalid count");
        }
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
        blk = blk->next.load(std::memory_order_acquire);
    }
}