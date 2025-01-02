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

class DirectUringRandomAccessFile : public DirectRandomAccessFile
{
public:
    explicit DirectUringRandomAccessFile(const std::string &file);

    static void RegisterBuffer(std::vector <std::shared_ptr<ByteBuffer>> buffers);

    static void RegisterBufferFromPool(std::vector <uint32_t> colIds);

    static void Initialize();

    static void Reset();

    std::shared_ptr <ByteBuffer> readAsync(int length, std::shared_ptr <ByteBuffer> buffer, int index);

    void readAsyncSubmit(int size);

    void readAsyncComplete(int size);

    ~DirectUringRandomAccessFile();

private:
    static thread_local struct io_uring *ring;
    static thread_local bool isRegistered;
    static thread_local struct iovec *iovecs;
    static thread_local uint32_t
    iovecSize;
};
#endif // DUCKDB_DIRECTURINGRANDOMACCESSFILE_H
