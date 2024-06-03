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
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stack>

#include "mq/shared_memory.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */
class SharedMemoryAllocator {
 public:
  SharedMemoryAllocator(std::shared_ptr<stm::SharedMemory> mem, long size,
                        long slot_size) {
    mem_ = mem;
    size_ = size;
    slot_size_ = slot_size;
    slot_num_ = size_ / slot_size_;
    for (int i = slot_num_ - 1; i >= 0; i--) {
      free_slots_.push(slot_size_ * i);
    }
  }

  long Allocate() {
    long pos = 0;
    std::unique_lock<std::mutex> lck(mu_);
    while (free_slots_.empty()) {
      if (flag_) {
        return -1;
      }
      cond_.wait(lck);
    }
    pos = free_slots_.top();
    free_slots_.pop();
    std::cout << "Allocate " << pos << std::endl;
    return pos;
  }

  void Release(long pos) {
    std::unique_lock<std::mutex> lck(mu_);
    free_slots_.push(pos);
    std::cout << "Release " << pos << std::endl;
    if (free_slots_.size() == 1) {
      cond_.notify_one();
    }
  }

 private:
  std::shared_ptr<stm::SharedMemory> mem_;
  long size_;
  long slot_size_;
  int slot_num_;
  std::stack<long> free_slots_;

  std::mutex mu_;
  std::condition_variable cond_;
  volatile bool flag_ = false;
};