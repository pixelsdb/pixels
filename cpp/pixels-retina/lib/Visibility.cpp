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
#define SET_BITMAP_BIT(bitmap, rowId) \
    ((bitmap)[(rowId) / 64] |= (1ULL << ((rowId) % 64)))
#endif

Visibility::Visibility() : baseTimestamp(-1UL) {
    memset(baseBitmap, 0, 4 * sizeof(uint64_t));
    head.store(nullptr, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
    tailUsed.store(0, std::memory_order_release);
}

Visibility::Visibility(uint64_t ts, const uint64_t bitmap[4])
    : baseTimestamp(ts) {
    memcpy(baseBitmap, bitmap, 4 * sizeof(uint64_t));
    head.store(nullptr, std::memory_order_release);
    tail.store(nullptr, std::memory_order_release);
    tailUsed.store(0, std::memory_order_release);
}

Visibility::~Visibility() {
    DeleteIndexBlock* blk = head.load(std::memory_order_acquire);
    while (blk) {
        DeleteIndexBlock* next = blk->next.load(std::memory_order_acquire);
        delete blk;
        blk = next;
    }
}

bool Visibility::deleteRecord(uint8_t rowId, uint64_t ts) {
    uint64_t item = makeDeleteIndex(rowId, ts);

    while (true) {
        DeleteIndexBlock* curTail = tail.load(std::memory_order_acquire);
        uint64_t used = tailUsed.load(std::memory_order_acquire);
        if (!curTail) { // empty list
            DeleteIndexBlock* newBlk = new DeleteIndexBlock();
            newBlk->next.store(nullptr, std::memory_order_relaxed);
            uint64_t expected = 0;
            if (!tailUsed.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
                delete newBlk;
                continue;
            }
            newBlk->items[0] = item;
            DeleteIndexBlock* expectedTail = nullptr;
            if (!tail.compare_exchange_strong(expectedTail, newBlk, std::memory_order_acq_rel)) {
                tailUsed.store(0, std::memory_order_release);
                delete newBlk;
                continue;
            }
            head.store(newBlk, std::memory_order_release);
            return true;
        } else {
            if (used < 8) {
                uint64_t oldUsed = tailUsed.fetch_add(1, std::memory_order_acquire);
                if (oldUsed < 8) {
                    curTail->items[oldUsed] = item;
                    return true;
                } else {
                    tailUsed.fetch_sub(1, std::memory_order_release);
                }
            }

            // curTail is full
            uint64_t oldVal = tailUsed.exchange(1, std::memory_order_acq_rel);
            if (oldVal < 8) {
                // another thread may have already added new block
                tailUsed.store(oldVal, std::memory_order_release);
                continue;
            }

            DeleteIndexBlock* newBlk = new DeleteIndexBlock();
            newBlk->next.store(nullptr, std::memory_order_relaxed);
            newBlk->items[0] = item;

            if (tail.load(std::memory_order_acquire) != curTail) {
                tailUsed.store(0, std::memory_order_release);
                delete newBlk;
                continue;
            }

            DeleteIndexBlock* expectedNext = nullptr;
            if (!curTail->next.compare_exchange_strong(expectedNext, newBlk, std::memory_order_acq_rel)) {
                tailUsed.store(oldVal, std::memory_order_release);
                delete newBlk;
                continue;
            }

            tail.compare_exchange_strong(curTail, newBlk, std::memory_order_acq_rel);
            return true;
        }
    }
    return false;
}

void Visibility::getVisibilityBitmap(uint64_t ts, uint64_t outBitmap[4]) const {
    if (ts < baseTimestamp) {
        throw std::runtime_error("need to read checkpoint from disk");
    }
    std::memcpy(outBitmap, baseBitmap, 4 * sizeof(uint64_t));
    if (ts == baseTimestamp) {
        return;
    }

    DeleteIndexBlock* blk = head.load(std::memory_order_acquire);
    if (!blk) {
        return;
    }

    DeleteIndexBlock* tailBlk = tail.load(std::memory_order_acquire);
    uint64_t usedInTail = tailUsed.load(std::memory_order_acquire);
    if (usedInTail > 8) {
        throw std::runtime_error("used in tail is greater than 8");
    }
    while (blk) {
        uint64_t count = (blk == tailBlk) ? usedInTail : 8;
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
        if (blk == tailBlk) {
            break;
        }
        blk = blk->next.load(std::memory_order_acquire);
    }
}

void Visibility::cleanUp(uint64_t ts) {
    while (true) {
        DeleteIndexBlock* oldHead = head.load(std::memory_order_acquire);
        if (!oldHead) {
            return;
        }

        DeleteIndexBlock* tailBlk = tail.load(std::memory_order_acquire);
        if ((oldHead == tailBlk) && (tailUsed.load(std::memory_order_acquire) != 8)) {
            return;
        }

        uint64_t item = oldHead->items[7];
        uint64_t delTs = extractTimestamp(item);
        if (delTs > ts) {
            return;
        }

        DeleteIndexBlock* newHead = oldHead->next.load(std::memory_order_acquire);
        if (oldHead == tailBlk) {
            head.store(nullptr, std::memory_order_release);
            tail.store(nullptr, std::memory_order_release);
            tailUsed.store(0, std::memory_order_release);
        } else {
            head.store(newHead, std::memory_order_release);
        }
        delete oldHead;
    }
}

