/*
/* @author whz 
/* @create 7/30/25.
*/
#include "physical/BufferPool/BufferPoolEntry.h"
#include "physical/BufferPool/Bitmap.h"

BufferPoolEntry::BufferPoolEntry(size_t size, int slice_size, std::shared_ptr<DirectIoLib> direct_lib,int offset,int ring_index)
    : size_(size), is_full_(false), next_free_(0),is_in_use_(false),offset_in_buffers_(offset),is_registered(false),ring_index(ring_index) {

  if (size == 0 || slice_size <= 0) {
    throw std::invalid_argument("Invalid buffer size or slice size");
  }
  if (!direct_lib) {
    throw std::invalid_argument("DirectIoLib pointer cannot be null");
  }

  const int slice_count = static_cast<int>(size + slice_size-1/ slice_size);
  bitmap_ = std::make_shared<Bitmap>(slice_count);

  buffer_ = direct_lib->allocateDirectBuffer(size_);
  if (!buffer_) {
    throw std::runtime_error("Failed to allocate direct buffer");
  }
}

size_t BufferPoolEntry::getSize() const {
  return size_;
}

std::shared_ptr<Bitmap> BufferPoolEntry::getBitmap() const {
  return bitmap_;
}


std::shared_ptr<ByteBuffer> BufferPoolEntry::getBuffer() const {
  return buffer_;
}


bool BufferPoolEntry::isFull() const {
  return is_full_;
}


int BufferPoolEntry::getNextFreeIndex() const {
  return next_free_;
}


int BufferPoolEntry::setNextFreeIndex(int index) {
  const int old_index = next_free_;
  next_free_ = index;
  is_full_ = (index > size_);
  if (is_full_) {
    return -1;
    // the buffer is full
  }
  return old_index;
}

uint64_t BufferPoolEntry::checkCol(uint32_t col) const {
  return nr_bytes_.find(col)==nr_bytes_.end() ? -1 : nr_bytes_.at(col);
}

void BufferPoolEntry::addCol(uint32_t colId,uint64_t bytes) {
  nr_bytes_[colId] =bytes;
}

bool BufferPoolEntry::isInUse() const {
  return is_in_use_;
}

void BufferPoolEntry::setInUse(bool in_use) {
  is_in_use_ = in_use;
}

int BufferPoolEntry::getOffsetInBuffers() const {
  return offset_in_buffers_;
}

void BufferPoolEntry::setOffsetInBuffers(int offset) {
  offset_in_buffers_=offset;
}

bool BufferPoolEntry::getIsRegistered() const {
  return is_registered;
}

void BufferPoolEntry::setIsRegistered(bool registered) {
  is_registered=registered;
}

int BufferPoolEntry::getRingIndex() const {
  return ring_index;
}

void BufferPoolEntry::setRingIndex(int ring_index) {
  ring_index=ring_index;
}

void BufferPoolEntry::reset() {
  is_full_ = false;
  next_free_=0;
  if (bitmap_) {
    bitmap_.reset();
  }
  nr_bytes_.clear();
  is_in_use_ = false;
// reset direct buffer?
}

BufferPoolEntry::~BufferPoolEntry() = default;