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

#ifndef PIXELS_RETINA_PATCHARR_H
#define PIXELS_RETINA_PATCHARR_H

#include <cstdint>
#include <cstddef>

class PatchArr {
public:
    PatchArr();
    ~PatchArr() = default;

    /**
     * Write a 32-byte checkpoint into the currentChunk at offset=0
     */
    uint8_t writeCheckPoint(const unsigned char* checkpoint);

    /**
     * Append 1 byte to the current chunk
     */
    void appendDeleteIndex(unsigned char rowId, uint8_t& outPatchIndex, uint16_t& outPatchOffset);

    /**
     * Read bytes from chunkArr[patchIndex] into dest
     */
    void readData(uint8_t patchIndex, uint16_t offsetEnd, unsigned char* dest, std::size_t destSize) const;

private:
    PatchArr(const PatchArr&) = delete;
    PatchArr& operator=(const PatchArr&) = delete;

    static const std::size_t CHUNK_SIZE = 64;
    static const std::size_t MAX_CHUNKS = 8;
    static const std::size_t CHECKPOINT_SIZE = 32;
    unsigned char chunkArr[MAX_CHUNKS][CHUNK_SIZE];
    uint8_t usedChunks;
    uint8_t currentChunk;
    uint16_t currentOffset;
};

#endif //PIXELS_RETINA_PATCHARR_H
