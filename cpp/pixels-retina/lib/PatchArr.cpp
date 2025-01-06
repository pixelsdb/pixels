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

#include "PatchArr.h"
#include <cstring>
#include <stdexcept>

PatchArr::PatchArr() {
    std::memset(chunkArr, 0, sizeof(chunkArr));
    usedChunks = 1;
    currentChunk = 0;
    currentOffset = 0;
}

uint8_t PatchArr::writeCheckPoint(const unsigned char *checkpoint) {
    if (currentOffset != 0) {
        if (usedChunks >= MAX_CHUNKS) {
            throw std::runtime_error("Cannot write checkpoint, no more chunks available");
        }
        if (currentOffset != CHUNK_SIZE - 1) {
            throw std::runtime_error("Current chunk is not full, cannot write checkpoint");
        }
        currentChunk = usedChunks;
        usedChunks++;
        currentOffset = 0;
        std::memset(chunkArr[currentChunk], 0, CHUNK_SIZE);
    }
    std::memcpy(&chunkArr[currentChunk][0], checkpoint, CHECKPOINT_SIZE);
    currentOffset = CHECKPOINT_SIZE;
    return currentChunk;
}

void PatchArr::appendDeleteIndex(unsigned char rowId, uint8_t &outPatchIndex, uint16_t &outPatchOffset) {
    if (currentChunk >= MAX_CHUNKS) {
        throw std::runtime_error("Cannot append delete index, no more chunks available");
    }

    if (currentOffset >= CHUNK_SIZE - 1) {
        if (usedChunks >= MAX_CHUNKS) {
            throw std::runtime_error("Cannot append delete index, no more chunks available");
        }
        currentChunk = usedChunks;
        usedChunks++;
        currentOffset = 0;
        std::memset(chunkArr[currentChunk], 0, CHUNK_SIZE);
    }

    chunkArr[currentChunk][currentOffset] = rowId;
    currentOffset++;
    outPatchIndex = currentChunk;
    outPatchOffset = currentOffset;
}

void PatchArr::readData(uint8_t patchIndex, uint16_t offsetEnd, unsigned char *dest, std::size_t destSize) const {
    if (patchIndex >= MAX_CHUNKS) {
        throw std::runtime_error("Invalid patch index");
    }
    if (offsetEnd >= CHUNK_SIZE) {
        throw std::runtime_error("Invalid offset");
    }
    std::size_t bytesToCopy = (offsetEnd < destSize) ? offsetEnd : destSize;
    std::memcpy(dest, chunkArr[patchIndex], bytesToCopy);
}