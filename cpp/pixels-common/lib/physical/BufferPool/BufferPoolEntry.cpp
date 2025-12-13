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
#include "physical/BufferPool/BufferPoolEntry.h"
#include "physical/BufferPool/Bitmap.h"

BufferPoolEntry::BufferPoolEntry(size_t size, int slice_size, std::shared_ptr<DirectIoLib> directLib, int offset,
                                 int ringIndex)
    : size_(size), isFull_(false), inexFree_(0), isInUse_(false), offsetInBuffers_(offset), isRegistered(false),
      ringIndex(ringIndex)
{
    if (size == 0 || slice_size <= 0)
    {
        throw std::invalid_argument("Invalid buffer size or slice size");
    }
    if (!directLib)
    {
        throw std::invalid_argument("DirectIoLib pointer cannot be null");
    }

    const int slice_count = static_cast<int>(size + slice_size - 1 / slice_size);
    // bitmap_ = std::make_shared<Bitmap>(slice_count);

    buffer_ = directLib->allocateDirectBuffer(size_, false);
    memset(buffer_->getPointer(), 0, buffer_->size());
    if (!buffer_)
    {
        throw std::runtime_error("Failed to allocate direct buffer");
    }
}

size_t BufferPoolEntry::getSize() const
{
    return size_;
}

// std::shared_ptr<Bitmap> BufferPoolEntry::getBitmap() const {
//   return bitmap_;
// }


std::shared_ptr<ByteBuffer> BufferPoolEntry::getBuffer() const
{
    return buffer_;
}


bool BufferPoolEntry::isFull() const
{
    return isFull_;
}


int BufferPoolEntry::getNextFreeIndex() const
{
    return inexFree_;
}


int BufferPoolEntry::setNextFreeIndex(int index)
{
    const int old_index = inexFree_;
    inexFree_ = index;
    isFull_ = (index > size_);
    if (isFull_)
    {
        return -1;
        // the buffer is full
    }
    return old_index;
}

uint64_t BufferPoolEntry::checkCol(uint32_t col) const
{
    return nrBytes_.find(col) == nrBytes_.end() ? -1 : nrBytes_.at(col);
}

void BufferPoolEntry::addCol(uint32_t colId, uint64_t bytes)
{
    nrBytes_[colId] = bytes;
}

bool BufferPoolEntry::isInUse() const
{
    return isInUse_;
}

void BufferPoolEntry::setInUse(bool in_use)
{
    isInUse_ = in_use;
}

int BufferPoolEntry::getOffsetInBuffers() const
{
    return offsetInBuffers_;
}

void BufferPoolEntry::setOffsetInBuffers(int offset)
{
    offsetInBuffers_ = offset;
}

bool BufferPoolEntry::getIsRegistered() const
{
    return isRegistered;
}

void BufferPoolEntry::setIsRegistered(bool registered)
{
    isRegistered = registered;
}

int BufferPoolEntry::getRingIndex() const
{
    return ringIndex;
}

void BufferPoolEntry::setRingIndex(int ringIndex)
{
    ringIndex = ringIndex;
}

void BufferPoolEntry::reset()
{
    isFull_ = false;
    inexFree_ = 0;
    // if (bitmap_) {
    //   bitmap_.reset();
    // }
    nrBytes_.clear();
    isInUse_ = false;
    // reset direct buffer?
}

BufferPoolEntry::~BufferPoolEntry() = default;
