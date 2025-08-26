/*
/* @author whz 
/* @create 7/30/25.
*/

#ifndef BUFFERPOOLENTRY_H
#define BUFFERPOOLENTRY_H
#include <memory>
#include <physical/natives/ByteBuffer.h>
#include <physical/natives/DirectIoLib.h>
#include "physical/BufferPool/Bitmap.h"
class BufferPoolEntry {
public:
explicit BufferPoolEntry(size_t size,int slice_size,std::shared_ptr<DirectIoLib> direct_lib,int offset,int ring_index);
  size_t getSize() const;
  std::shared_ptr<Bitmap> getBitmap() const;
  std::shared_ptr<ByteBuffer> getBuffer() const;
  bool isFull() const;
  int getNextFreeIndex() const;
  int setNextFreeIndex(int index);
  ~BufferPoolEntry();
  uint64_t checkCol(uint32_t) const;
  void addCol(uint32_t colId,uint64_t bytes);
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

#endif //BUFFERPOOLENTRY_H
