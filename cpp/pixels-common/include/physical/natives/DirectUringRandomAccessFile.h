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
 * @create 2023-05-28
 */
#ifndef DUCKDB_DIRECTURINGRANDOMACCESSFILE_H
#define DUCKDB_DIRECTURINGRANDOMACCESSFILE_H

#include "liburing.h"
#include "liburing/io_uring.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "exception/InvalidArgumentException.h"
#include "DirectIoLib.h"
#include "physical/BufferPool.h"
#include "unordered_set"
#include <mutex>
#include "utils/MutexTracker.h"

class DirectUringRandomAccessFile : public DirectRandomAccessFile
{
public:
    explicit DirectUringRandomAccessFile(const std::string& file);

    static void RegisterBuffer(std::vector<std::shared_ptr<ByteBuffer>> buffers);

    static void RegisterBufferFromPool(std::vector<uint32_t> colIds);

    static void Initialize();

    static void Reset();

    static bool RegisterMoreBuffer(int index, std::vector<std::shared_ptr<ByteBuffer>> buffers);

    std::shared_ptr<ByteBuffer> readAsync(int length, std::shared_ptr<ByteBuffer> buffer, int index, int ring_index,
                                          int start_offset);

    void readAsyncSubmit(std::unordered_map<int, uint32_t> sizes, std::unordered_set<int> ring_indexs);

    void readAsyncComplete(std::unordered_map<int, uint32_t> sizes, std::unordered_set<int> ring_indexs);

    void seekByIndex(long offset, int index);

    static struct io_uring* getRing(int index);

    ~DirectUringRandomAccessFile();

private:
    // thread_local
    static std::mutex mutex_;
    static thread_local bool isRegistered;
    // static MutexTracker g_mutex_tracker;
    // static TrackedMutex g_mutex;
    static thread_local std::vector<struct io_uring*> ring_vector;
    static thread_local std::vector<struct iovec*> iovecs_vector;
    static thread_local uint32_t iovecSize;
    static thread_local std::vector<long> offsets_vector;
};
#endif // DUCKDB_DIRECTURINGRANDOMACCESSFILE_H
