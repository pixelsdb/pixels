/*
 * Copyright 2017-2019 PixelsDB.
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
#pragma once
#include <vector>

#include "column_vector.h"
#include "memory/concurrent_arena.h"

class BinaryColumnVector : public ColumnVector {
 public:
  BinaryColumnVector(int max_length) : ColumnVector(max_length) {
    value_type_ = ValueType::BYTES;
    starts_ = new char*[max_length];
    lengths_ = new int[max_length];
    allocator_ = new rocksdb::ConcurrentArena();
  }

  ~BinaryColumnVector() {
    if (starts_) {
      delete[] starts_;
      starts_ = nullptr;
    }
    if (lengths_) {
      delete[] lengths_;
      lengths_ = nullptr;
    }
    if (allocator_) {
      delete allocator_;
      allocator_ = nullptr;
    }
  }

  bool Add(int index, Slice& value) {
    char* pos = allocator_->Allocate(value.size());
    memcpy(pos, value.data_, value.size());
    starts_[index] = pos;
    lengths_[index] = value.size();
    cur_length_ = std::max(index + 1, cur_length_);
    return true;
  };

  virtual bool FilteredFlush(std::shared_ptr<stm::SharedMemory> mem, long& pos,
                             int indices[], int len) {
    mem->setShort(pos, static_cast<short>(value_type_));  // type
    pos += sizeof(short);

    int* offsets = new int[len];  // rid -> logical overall pos
    offsets[0] = 0;
    for (int i = 1; i < len; i++) {
      offsets[i] = offsets[i - 1] + lengths_[indices[i - 1]];
    }
    mem->setInt(pos, len * sizeof(int));  // offset/length array length
    pos += sizeof(int);

    mem->setBytes(pos, (stm::byte*)offsets, 0, len * sizeof(int));  // offset
    pos += len * sizeof(int);

    for (int i = 0; i < len; i++) {  // len
      mem->setInt(pos, lengths_[indices[i]]);
      pos += sizeof(int);
    }

    // record data length pos
    int data_len_pos = pos;
    //   mem->setInt(pos, total_length_); // data length
    pos += sizeof(int);
    for (int i = 0; i < len; i++) {
      int index = indices[i];
      mem->setBytes(pos, (stm::byte*)starts_[index], 0, lengths_[index]);
      pos += lengths_[index];
    }

    mem->setInt(data_len_pos,
                pos - data_len_pos - sizeof(int));  // set data length

    delete[] offsets;
    return true;
  };

  bool SortedFlush(std::shared_ptr<stm::SharedMemory> mem, long& pos,
                   int indices[]) {
    mem->setShort(pos, static_cast<short>(value_type_));  // type
    pos += sizeof(short);

    int* offsets = new int[cur_length_];  // rid -> logical overall pos
    offsets[0] = 0;
    for (int i = 1; i < cur_length_; i++) {
      offsets[i] = offsets[i - 1] + lengths_[indices[i - 1]];
    }
    mem->setInt(pos, cur_length_ * sizeof(int));
    pos += sizeof(int);

    mem->setBytes(pos, (stm::byte*)offsets, 0,
                  cur_length_ * sizeof(int));  // offset
    pos += cur_length_ * sizeof(int);

    for (int i = 0; i < cur_length_; i++) {  // len
      mem->setInt(pos, lengths_[indices[i]]);
      pos += sizeof(int);
    }

    // record data length pos
    int data_len_pos = pos;
    //   mem->setInt(pos, total_length_); // data length
    pos += sizeof(int);
    for (int i = 0; i < cur_length_; i++) {
      int index = indices[i];
      mem->setBytes(pos, (stm::byte*)starts_[index], 0, lengths_[index]);
      pos += lengths_[index];
    }

    mem->setInt(data_len_pos,
                pos - data_len_pos - sizeof(int));  // set data length

    delete[] offsets;
    return true;
  };

  // Slice operator[](int index) {
  //   return Slice(buffers_[blocks_[index]]->buffer_ + starts_[index],
  //                lengths_[index]);
  // }

 private:
  char** starts_;
  int* lengths_;  // rid -> data length
  rocksdb::ConcurrentArena* allocator_;
};