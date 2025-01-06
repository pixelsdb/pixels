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

#ifndef PIXELS_RETINA_EPOCHLIST_H
#define PIXELS_RETINA_EPOCHLIST_H

#include <atomic>
#include <cstddef>
#include <cstdint>

/**
 * Describes a single epoch with a timestamp and the patch index and offset in the patch array.
 * An epoch corresponds to patchArr[patchIndex][0..patchOffset).
 * Sizeof = 16 bytes
 */
struct EpochInfo {
    uint8_t patchIndex;     // 0..7
    uint16_t patchOffset;   // 33..64
    uint64_t epochTs;
};

/**
 * One epoch block is 4096 bytes and contains 256 epochs.
 */
static const std::size_t EPOCH_BLOCK_CAPACITY = 256;
struct EpochBlock {
    uint32_t count;         // 0..256
    uint64_t minTs;
    uint64_t maxTs;
    EpochInfo epochs[EPOCH_BLOCK_CAPACITY];
};

/**
 * A single linked list of blocks, each with up to EPOCH_BLOCK_CAPACITY epochs.
 * We only append new epochs to the tail block; if full, create a new block.
 * We can remove old blocks from the head if needed;
 */
class EpochList {
public:
    EpochList();
    ~EpochList();

    /**
     * Insert a new epoch into the tail block, returns pointer to the new epoch.
     */
    EpochInfo* addEpoch(uint8_t pathIndex, uint16_t patchOffset, uint64_t epochTs);

    /**
     * Find epoch by exact epochTs, returns pointer or nullptr if not found.
     */
    EpochInfo* findEpoch(uint64_t epochTs);

    /**
     * Remove blocks whose maxTs < cutoffTs from the head.
     */
    void cleanOldEpochs(uint64_t cutoffTs);

private:
    EpochList(const EpochList&) = delete;
    EpochList& operator=(const EpochList&) = delete;

    struct BlockNode {
        EpochBlock* block;
        std::atomic<BlockNode*> next;

        BlockNode(EpochBlock* b)
            : block(b), next(nullptr) {}
    };
    std::atomic<BlockNode*> head;
    std::atomic<BlockNode*> tail;

    /**
     * Create a new EpochBlock on the heap with default fields.
     */
    EpochBlock* allocateBlock();

    /**
     * Create a new block, appends it to the tail, returns pointer to the new block.
     */
    EpochBlock* addBlock();

    /**
     * Read the current tail node, return block or nullptr if tail is nullptr.
     */
    EpochBlock* getTailBlock() const;
};

#endif //PIXELS_RETINA_EPOCHLIST_H
