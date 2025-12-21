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

#include "exception/InvalidArgumentException.h"
#include "physical/BufferPool/Bitmap.h"
#include "physical/BufferPool/BufferPoolEntry.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectIoLib.h"
#include "utils/ColumnSizeCSVReader.h"
#include <cstdio>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
// when allocating buffer pool, we use the size of the first pxl file. Consider
// that the remaining pxl file has larger size than the first file, we allocate
// some extra size (10MB) to each column.
// TODO: how to evaluate the maximal pool size
#define EXTRA_POOL_SIZE 10 * 1024 * 1024

class DirectUringRandomAccessFile;

// This class is global class. The variable is shared by each thread
class BufferPool
{
public:

    class BufferPoolManagedEntry
    {
    public:
        enum class State
        {
            InitizaledNotAllocated,
            AllocatedAndInUse,
            UselessButNotFree
        };

    private:
        std::shared_ptr<BufferPoolEntry> bufferPoolEntry;
        int ringIndex;
        size_t currentSize;
        int offset;
        State state;

    public:
        BufferPoolManagedEntry(std::shared_ptr<BufferPoolEntry> entry, int ringIdx,
                               size_t currSize, off_t off)
            : bufferPoolEntry(std::move(entry)), ringIndex(ringIdx),
              currentSize(currSize), offset(off),
              state(State::InitizaledNotAllocated)
        {
        }

        std::shared_ptr<BufferPoolEntry> getBufferPoolEntry() const
        {
            return bufferPoolEntry;
        }

        int getRingIndex() const
        {
            return ringIndex;
        }

        void setRingIndex(int index)
        {
            ringIndex = index;
        }

        size_t getCurrentSize() const
        {
            return currentSize;
        }

        void setCurrentSize(size_t size)
        {
            currentSize = size;
        }

        int getOffset() const
        {
            return offset;
        }

        void setOffset(int off)
        {
            offset = off;
        }

        State getStatus() const
        {
            return state;
        }

        void setStatus(State newStatus)
        {
            state = newStatus;
        }
    };

    static void Initialize(std::vector<uint32_t> colIds,
                           std::vector<uint64_t> bytes,
                           std::vector<std::string> columnNames);

    static void InitializeBuffers();

    static std::shared_ptr<ByteBuffer> GetBuffer(uint32_t colId, uint64_t byte,
                                                 std::string columnName);

    static int64_t GetBufferId();

    static void Switch();

    static void Reset();

    static std::shared_ptr<BufferPoolEntry> AddNewBuffer(size_t size);

    static int getRingIndex(uint32_t colId);

    static std::shared_ptr<ByteBuffer> AllocateNewBuffer(
        std::shared_ptr<BufferPoolManagedEntry> currentBufferManagedEntry,
        uint32_t colId, uint64_t byte, std::string columnName);

    static std::shared_ptr<ByteBuffer> ReusePreviousBuffer(
        std::shared_ptr<BufferPoolManagedEntry> currentBufferManagedEntry,
        uint32_t colId, uint64_t byte, std::string columnName);

    static void PrintStats()
    {
        // Get the ID of the current thread
        std::thread::id tid = std::this_thread::get_id();

        // Print global buffer usage: used size / free size
        // Convert thread ID to integer for readability using hash
        printf("Thread %zu -> Global buffer usage: %ld / %ld\n",
               std::hash<std::thread::id>{}(tid), globalUsedSize,
               globalFreeSize);

        // Print thread-local statistics for Buffer0
        printf("Thread %zu -> Buffer0 usage: %zu, Buffer count: %d\n",
               std::hash<std::thread::id>{}(tid), threadLocalUsedSize[0],
               threadLocalBufferCount[0]);

        // Print thread-local statistics for Buffer1
        printf("Thread %zu -> Buffer1 usage: %zu, Buffer count: %d\n",
               std::hash<std::thread::id>{}(tid), threadLocalUsedSize[1],
               threadLocalBufferCount[1]);
    }
private:
    BufferPool() = default;
    // global
    static std::mutex bufferPoolMutex;

    // thread local
    static thread_local bool isInitialized;
    static thread_local std::vector<std::shared_ptr<BufferPoolEntry>>
    registeredBuffers[2];
    static thread_local long globalUsedSize;
    static thread_local long globalFreeSize;
    static thread_local std::shared_ptr<DirectIoLib> directIoLib;
    static thread_local int nextRingIndex;
    static thread_local std::shared_ptr<BufferPoolEntry>
    nextEmptyBufferPoolEntry[2];
    static thread_local int colCount;
    static thread_local int currBufferIdx;
    static thread_local int nextBufferIdx;
    static thread_local std::map<uint32_t, std::shared_ptr<ByteBuffer>>
    buffersAllocated[2];
    friend class DirectUringRandomAccessFile;

    static thread_local std::unordered_map<
        uint32_t, std::shared_ptr<BufferPoolManagedEntry>>
    ringBufferMap[2];

    static thread_local size_t threadLocalUsedSize[2];
    static thread_local int threadLocalBufferCount[2];
};
#endif // DUCKDB_BUFFERPOOL_H
