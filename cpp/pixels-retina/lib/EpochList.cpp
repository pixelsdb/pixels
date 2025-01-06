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

#include "EpochList.h"
#include <new>
#include <cstring>

EpochBlock* EpochList::allocateBlock() {
    EpochBlock* blk = new(std::nothrow) EpochBlock;
    if (!blk) {
        return nullptr;
    }
    blk->count = 0;
    blk->minTs = 0;
    blk->maxTs = 0;
    return blk;
}

EpochList::EpochList()
{
    head.store(nullptr, std::memory_order_relaxed);
    tail.store(nullptr, std::memory_order_relaxed);
}

EpochList::~EpochList() {
    BlockNode* node = head.load(std::memory_order_relaxed);
    while (node) {
        BlockNode* tmp = node;
        node = node->next.load(std::memory_order_relaxed);
        delete tmp->block;
        delete tmp;
    }
    head.store(nullptr, std::memory_order_relaxed);
    tail.store(nullptr, std::memory_order_relaxed);
}

EpochBlock* EpochList::getTailBlock() const {
    BlockNode* tailNode = tail.load(std::memory_order_acquire);
    return tailNode ? tailNode->block : nullptr;
}

EpochBlock* EpochList::addBlock() {
    EpochBlock* blk = allocateBlock();
    if (!blk) {
        return nullptr;
    }

    BlockNode* newNode = new(std::nothrow) BlockNode(blk);
    if (!newNode) {
        delete blk;
        return nullptr;
    }

    while(true) {
        BlockNode* tailNode = tail.load(std::memory_order_acquire);
        if (!tail) {
            BlockNode* expectedNull = nullptr;
            if (head.compare_exchange_weak(expectedNull, newNode, std::memory_order_acq_rel, std::memory_order_acquire)) {
                BlockNode* nullTail = nullptr;
                tail.compare_exchange_strong(nullTail, newNode, std::memory_order_acq_rel, std::memory_order_acquire);
                return blk;
            }
            continue;
        }
        BlockNode* tailNext = tailNode->next.load(std::memory_order_acquire);
        if (tailNext) {
            tail.compare_exchange_weak(tailNode, tailNext, std::memory_order_acq_rel, std::memory_order_acquire);
            continue;
        }
        if (tailNode->next.compare_exchange_weak(tailNext, newNode, std::memory_order_acq_rel, std::memory_order_acquire)) {
            tail.compare_exchange_strong(tailNode, newNode, std::memory_order_acq_rel, std::memory_order_acquire);
            return blk;
        }
    }
}

EpochInfo* EpochList::addEpoch(uint8_t pathIndex, uint16_t patchOffset, uint64_t epochTs) {
    while(true) {
        EpochBlock* blk = getTailBlock();
        if (!blk) {
            blk = addBlock();
            if (!blk) {
                return nullptr;
            }
            continue;
        }
        std::size_t idx = blk->count;
        if (idx >= EPOCH_BLOCK_CAPACITY) {
            blk = addBlock();
            if (!blk) {
                return nullptr;
            }
            continue;
        }
        blk->count = idx + 1;

        EpochInfo& info = blk->epochs[idx];
        info.epochTs = epochTs;
        info.patchIndex = pathIndex;
        info.patchOffset = patchOffset;

        if (idx == 0) {
            blk->minTs = epochTs;
            blk->maxTs = epochTs;
        } else {
            if (epochTs < blk->minTs) {
                blk->minTs = epochTs;
            }
            if (epochTs > blk->maxTs) {
                blk->maxTs = epochTs;
            }
        }
        return &info;
    }
}

EpochInfo* EpochList::findEpoch(uint64_t epochTs) {
    BlockNode* cur = head.load(std::memory_order_acquire);
    while (cur) {
        EpochBlock* blk = cur->block;
        if (blk->count > 0 && epochTs >= blk->minTs && epochTs <= blk->maxTs) {
            for (std::size_t i = 0; i < blk->count; i++) {
                EpochInfo& info = blk->epochs[i];
                if (info.epochTs == epochTs) {
                    return &info;
                }
            }
            return nullptr;
        }
        cur = cur->next.load(std::memory_order_acquire);
    }
    return nullptr;
}

void EpochList::cleanOldEpochs(uint64_t cutoffTs) {
    while (true) {
        BlockNode* oldHead = head.load(std::memory_order_acquire);
        if (!oldHead) {
            return;
        }
        EpochBlock* blk = oldHead->block;
        if (blk->count > 0 && blk->maxTs >= cutoffTs) {
            return;
        }
        BlockNode* nextNode = oldHead->next.load(std::memory_order_acquire);
        if (head.compare_exchange_weak(oldHead, nextNode, std::memory_order_acq_rel, std::memory_order_acquire)) {
            delete blk;
            delete oldHead;
        } else {
            continue;
        }
    }
}