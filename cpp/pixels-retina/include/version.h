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
#include <cstring>
#include <unordered_map>

#include "const.h"
#include "mq/shared_memory.h"
#include "states.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */

#define BYTE_TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c\n"
#define BYTE_TO_BINARY(byte)                                \
  (byte & 0x80 ? '1' : '0'), (byte & 0x40 ? '1' : '0'),     \
      (byte & 0x20 ? '1' : '0'), (byte & 0x10 ? '1' : '0'), \
      (byte & 0x08 ? '1' : '0'), (byte & 0x04 ? '1' : '0'), \
      (byte & 0x02 ? '1' : '0'), (byte & 0x01 ? '1' : '0')

void PrintBinary(char byte) {
  printf(BYTE_TO_BINARY_PATTERN, BYTE_TO_BINARY(byte));
}

void PrintBinary(uint16_t num) {
  PrintBinary(*(char*)&num);
  PrintBinary(*((char*)&num + 1));
  std::cout << std::endl;
}

class Version {
 public:
  Version(int size) {
    row_num_ = size;
    vector_length_ = (row_num_ >> 6) + (0 != (row_num_ & ((1 << 6) - 1)));
    bits1_ = new uint64_t[vector_length_];
    bits2_ = new uint64_t[vector_length_];
    memset(bits1_, 0, vector_length_ * sizeof(uint64_t));
    memset(bits2_, 0, vector_length_ * sizeof(uint64_t));
  }

  Version(long* delete_timestamps, long lwm, int size) : Version(size) {
    for (int i = 0; i < size; i++) {
      if (delete_timestamps[i] != 0) {
        if (delete_timestamps[i] < lwm) {
          SetState(i, State::DELETED);
        } else {
          SetDeleteIntent(i, delete_timestamps[i]);
        }
      }
    }
  }

  Version(long* delete_timestamps, long lwm, int* indices, int size)
      : Version(size) {
    for (int i = 0; i < size; i++) {
      if (delete_timestamps[indices[i]] != 0) {
        if (delete_timestamps[indices[i]] < lwm) {
          SetState(i, State::DELETED);
        } else {
          SetDeleteIntent(i, delete_timestamps[indices[i]]);
        }
      }
    }
  }

  ~Version() {
    if (bits1_) {
      delete[] bits1_;
      bits1_ = nullptr;
    }
    if (bits2_) {
      delete[] bits2_;
      bits2_ = nullptr;
    }
  }

  void SetDeleteIntent(int row_id, int ts) {
    // update timestamps
    delete_timestamps_[row_id] = ts;
    // update bitmap
    SetState(row_id, State::DELETE_INTENT);
  }

  void ClearDeleted(long lwm) {
    // scan timestamps
    // if less than lower water mark, delete timestamp, update bitmap
    for (auto it = delete_timestamps_.begin();
         it != delete_timestamps_.end();) {
      if (it->second <= lwm) {
        SetState(it->first, State::DELETED);
        it = delete_timestamps_.erase(it);
      } else {
        it++;
      }
    }
  }

  void GetVisibility(long ts, std::shared_ptr<stm::SharedMemory> mem,
                     long& pos) {
    int temp = 0;
    int rid = 0;
    mem->setInt(pos, vector_length_ * sizeof(uint64_t));  // length
    pos += sizeof(int);
    for (int i = 0; i < vector_length_; i++) {
      uint64_t& num1 = bits1_[i];
      uint64_t& num2 = bits2_[i];
      uint64_t res = 0;
      for (int k = 0; k < 64; k++) {
        State state = static_cast<State>((((num1 >> k) & 1ull) << 1ull) +
                                         ((num2 >> k) & 1ull));
        if (state == State::NEW ||
            (state == State::DELETE_INTENT && delete_timestamps_[rid] > ts)) {
          SetOne(res, k);
        } else {
          std::cout << "invisible " << rid << std::endl;
        }
        rid++;
      }
      mem->setLong(pos, res);
      pos += sizeof(uint64_t);
    }
  }

  int GetHashmapSize() const { return delete_timestamps_.size(); }

 private:
  int row_num_ = 0;
  int vector_length_ = 0;
  uint64_t* bits1_;                                  // bitmap0
  uint64_t* bits2_;                                  // bitmap1
  std::unordered_map<int, long> delete_timestamps_;  // hashmap

  void SetBit(uint16_t& num, uint16_t n, uint16_t x) {
    num ^= (-x ^ num) & (1ull << n);
  }

  void SetOne(uint64_t& num, uint64_t n) { num = num | (1ull << n); }

  void SetState(int row_id, State state) {
    uint64_t& num1 = bits1_[row_id >> 6];
    uint64_t& num2 = bits2_[row_id >> 6];
    uint64_t offset = row_id & 63;
    switch (state) {
      case State::DELETE_INTENT:
        SetOne(num1, offset);
        break;
      case State::DELETED:
        SetOne(num1, offset);
        SetOne(num2, offset);
        break;
      default:
        break;
    }
  }
};
