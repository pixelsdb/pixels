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

/*
 * @author whz
 * @create 2025-07-30
 */

#ifndef BUFFERPOOLENTRY_H
#define BUFFERPOOLENTRY_H
#include "physical/BufferPool/Bitmap.h"
#include <memory>
#include <physical/natives/ByteBuffer.h>
#include <physical/natives/DirectIoLib.h>

class BufferPoolEntry
{
public:
    explicit BufferPoolEntry(size_t size, int slice_size,
                             std::shared_ptr<DirectIoLib> direct_lib, int offset,
                             int ring_index);
    size_t getSize() const;
    std::shared_ptr<Bitmap> getBitmap() const;
    std::shared_ptr<ByteBuffer> getBuffer() const;
    bool isFull() const;
    int getNextFreeIndex() const;
    int setNextFreeIndex(int index);
    ~BufferPoolEntry();
    uint64_t checkCol(uint32_t) const;
    void addCol(uint32_t colId, uint64_t bytes);
    bool isInUse() const;
    void setInUse(bool in_use);
    int getOffsetInBuffers() const;
    void setOffsetInBuffers(int offset);
    bool getIsRegistered() const;
    void setIsRegistered(bool registered);
    int getRingIndex() const;
    void setRingIndex(int ring_index);
    void reset();

private:
    size_t size_;
    std::shared_ptr<Bitmap> bitmap_;
    std::shared_ptr<ByteBuffer> buffer_;
    bool is_full_;
    int next_free_;
    std::map<uint32_t, uint64_t> nr_bytes_;
    bool is_in_use_;
    int offset_in_buffers_;
    bool is_registered;
    int ring_index;
};

#endif // BUFFERPOOLENTRY_H
