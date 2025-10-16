/*
 * Copyright 2023 PixelsDB.
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
 * @author liyu
 * @create 2023-05-25
 */
#ifndef DUCKDB_BUFFERPOOL_H
#define DUCKDB_BUFFERPOOL_H

#include <iostream>
#include <vector>
#include "physical/natives/ByteBuffer.h"
#include <memory>
#include "physical/natives/DirectIoLib.h"
#include "exception/InvalidArgumentException.h"
#include "utils/ColumnSizeCSVReader.h"
#include <map>
#include "physical/BufferPool/Bitmap.h"
#include "physical/BufferPool/BufferPoolEntry.h"
#include <mutex>
#include <thread>
#include <cstdio>
// when allocating buffer pool, we use the size of the first pxl file. Consider that
// the remaining pxl file has larger size than the first file, we allocate some extra
// size (10MB) to each column.
// TODO: how to evaluate the maximal pool size
#define EXTRA_POOL_SIZE 10*1024*1024

class DirectUringRandomAccessFile;
// This class is global class. The variable is shared by each thread
class BufferPool
{
public:
  // 嵌套子类，用于管理缓冲区池条目及其属性
  class BufferPoolManagedEntry {
  public:
    enum class State{
      InitizaledNotAllocated,
      AllocatedAndInUse,
      UselessButNotFree
    };
  private:
    std::shared_ptr<BufferPoolEntry> bufferPoolEntry;  // 指向缓冲区池条目的智能指针
    int ring_index;                                   // 环形缓冲区索引
    size_t current_size;                              // 当前使用大小
    int offset;                                     // 偏移量
    State state;



  public:

    BufferPoolManagedEntry(std::shared_ptr<BufferPoolEntry> entry,
                          int ringIdx,
                          size_t currSize,
                          off_t off)
        : bufferPoolEntry(std::move(entry)),
          ring_index(ringIdx),
          current_size(currSize),
          offset(off) ,
          state(State::InitizaledNotAllocated){}

    std::shared_ptr<BufferPoolEntry> getBufferPoolEntry() const {
      return bufferPoolEntry;
    }

    int getRingIndex() const {
      return ring_index;
    }

    void setRingIndex(int index) {
      ring_index = index;
    }

    size_t getCurrentSize() const {
      return current_size;
    }

    void setCurrentSize(size_t size) {
      current_size = size;
    }

    int getOffset() const {
      return offset;
    }

    void setOffset(int off) {
      offset = off;
    }

    // 获取当前状态
    State getStatus() const {
      return state;
    }

    // 设置状态
    void setStatus(State newStatus) {
      state = newStatus;
    }
  };

    static void
    Initialize(std::vector <uint32_t> colIds, std::vector <uint64_t> bytes, std::vector <std::string> columnNames);

    static void
    InitializeBuffers();

    static std::shared_ptr <ByteBuffer> GetBuffer(uint32_t colId,uint64_t byte,std::string columnName);

    static int64_t GetBufferId();

    static void Switch();

    static void Reset();

    static std::shared_ptr<BufferPoolEntry> AddNewBuffer(size_t size);

    static int getRingIndex(uint32_t colId);

    static std::shared_ptr<ByteBuffer> AllocateNewBuffer(std::shared_ptr<BufferPoolManagedEntry> currentBufferManagedEntry, uint32_t colId,uint64_t byte,std::string columnName);

    static std::shared_ptr<ByteBuffer> ReusePreviousBuffer(std::shared_ptr<BufferPoolManagedEntry> currentBufferManagedEntry,uint32_t colId,uint64_t byte,std::string columnName);

  static void PrintStats() {
    // 打印当前线程 ID
    std::thread::id tid = std::this_thread::get_id();

    printf("线程 %zu -> 全局缓冲区使用: %ld / %ld\n",
           std::hash<std::thread::id>{}(tid), // 转换成整数便于阅读
           global_used_size, global_free_size);

    // 线程局部统计
    printf("线程 %zu -> Buffer0使用: %zu, 缓冲区数量: %d\n",
           std::hash<std::thread::id>{}(tid),
           thread_local_used_size[0], thread_local_buffer_count[0]);

    printf("线程 %zu -> Buffer1使用: %zu, 缓冲区数量: %d\n",
           std::hash<std::thread::id>{}(tid),
           thread_local_used_size[1], thread_local_buffer_count[1]);
  }
private:
    BufferPool() = default;
  // global
  static std::mutex bufferPoolMutex;

  // thread local
  static thread_local bool isInitialized;
  static thread_local std::vector <std::shared_ptr<BufferPoolEntry>> registeredBuffers[2];
  static thread_local long global_used_size;
  static thread_local long global_free_size;
  static thread_local std::shared_ptr <DirectIoLib> directIoLib;
  static thread_local int nextRingIndex;
  static thread_local std::shared_ptr<BufferPoolEntry> nextEmptyBufferPoolEntry[2];
    static thread_local int colCount;

    static thread_local int currBufferIdx;
    static thread_local int nextBufferIdx;
    static thread_local std::map <uint32_t, std::shared_ptr<ByteBuffer>> buffersAllocated[2];
    friend class DirectUringRandomAccessFile;

    static thread_local std::unordered_map<uint32_t, std::shared_ptr<BufferPoolManagedEntry>> ringBufferMap[2];



  static thread_local size_t thread_local_used_size[2];  // 线程已使用大小
  static thread_local int thread_local_buffer_count[2];   // 线程持有的缓冲区数量
};
#endif // DUCKDB_BUFFERPOOL_H
