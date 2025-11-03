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
#include "physical/natives/DirectUringRandomAccessFile.h"

#include <duckdb/storage/buffer/buffer_pool.hpp>

// global
std::mutex DirectUringRandomAccessFile::mutex_;
// thread local
thread_local bool DirectUringRandomAccessFile::isRegistered = false;
thread_local std::vector<struct io_uring*>
DirectUringRandomAccessFile::ring_vector;
thread_local std::vector<struct iovec*>
DirectUringRandomAccessFile::iovecs_vector;
thread_local std::vector<long> DirectUringRandomAccessFile::offsets_vector;
thread_local uint32_t DirectUringRandomAccessFile::iovecSize = 0;

DirectUringRandomAccessFile::DirectUringRandomAccessFile(
    const std::string& file)
    : DirectRandomAccessFile(file)
{
}

void DirectUringRandomAccessFile::RegisterBufferFromPool(
    std::vector<uint32_t> colIds)
{
    if (!isRegistered)
    {
        std::vector<std::shared_ptr<ByteBuffer>> tmpBuffers;
        auto ring = DirectUringRandomAccessFile::getRing(0);
        struct iovec* iovecs = nullptr;
        for (auto buffers : ::BufferPool::registeredBuffers)
        {
            for (auto buffer : buffers)
            {
                buffer->setIsRegistered(true);
                tmpBuffers.emplace_back(buffer->getBuffer());
            }
        }
        iovecs = (iovec*)calloc(tmpBuffers.size(), sizeof(struct iovec));
        iovecSize = tmpBuffers.size();
        for (auto i = 0; i < tmpBuffers.size(); i++)
        {
            auto buffer = tmpBuffers.at(i);
            iovecs[i].iov_base = buffer->getPointer();
            iovecs[i].iov_len = buffer->size();
        }
        iovecs_vector.emplace_back(iovecs);
        int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
        if (ret != 0)
        {
            std::cerr << "io_uring_register_buffers failed: " << strerror(errno)
                << ", ret=" << ret << std::strerror(ret) << std::endl;
            throw InvalidArgumentException(
                "DirectUringRandomAccessFile::RegisterBufferFromPool: register "
                "buffer fails. ");
        }
        isRegistered = true;
    }
}

void DirectUringRandomAccessFile::RegisterBuffer(
    std::vector<std::shared_ptr<ByteBuffer>> buffers)
{
    if (!isRegistered)
    {
        auto ring = DirectUringRandomAccessFile::getRing(0);
        struct iovec* iovecs = nullptr;
        iovecs = (iovec*)calloc(buffers.size(), sizeof(struct iovec));
        iovecSize = buffers.size();
        for (auto i = 0; i < buffers.size(); i++)
        {
            auto buffer = buffers.at(i);
            iovecs[i].iov_base = buffer->getPointer();
            iovecs[i].iov_len = buffer->size();
            memset(iovecs[i].iov_base, 0, buffer->size());
        }
        ring_vector.emplace_back(ring);
        iovecs_vector.emplace_back(iovecs);
        int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
        if (ret != 0)
        {
            throw InvalidArgumentException("DirectUringRandomAccessFile::"
                "RegisterBuffer: register buffer fails. ");
        }
        isRegistered = true;
    }
}

void DirectUringRandomAccessFile::Initialize()
{
    std::lock_guard<std::mutex> lock(mutex_);
    getRing(0);
}

void DirectUringRandomAccessFile::Reset()
{
    // Important! Because sometimes ring is nullptr here.
    // For example, two threads A and B share the same global state. If A finish
    // all files while B just starts, B would execute Reset function from
    // InitLocal. If we don't set this 'if' branch, ring would be double freed.
    for (auto ring : ring_vector)
    {
        if (ring != nullptr)
        {
            // We don't use this function anymore since it slows down the speed
            //		if(io_uring_unregister_buffers(ring) != 0) {
            //			throw
            //InvalidArgumentException("DirectUringRandomAccessFile::UnregisterBuffer:
            //unregister buffer fails. ");
            //		}
            io_uring_queue_exit(ring);
            delete (ring);
            ring = nullptr;
            isRegistered = false;
        }
    }
    ring_vector.clear();
    for (auto iovecs : iovecs_vector)
    {
        if (iovecs != nullptr)
        {
            free(iovecs);
            iovecs = nullptr;
        }
    }
    iovecs_vector.clear();
}

bool DirectUringRandomAccessFile::RegisterMoreBuffer(
    int index, std::vector<std::shared_ptr<ByteBuffer>> buffers)
{
    assert(isRegistered);
    auto ring = DirectUringRandomAccessFile::getRing(index);
    struct iovec* iovecs = nullptr;
    iovecs = (iovec*)calloc(buffers.size(), sizeof(struct iovec));
    iovecSize = buffers.size();
    for (auto i = 0; i < buffers.size(); i++)
    {
        auto buffer = buffers.at(i);
        iovecs[i].iov_base = buffer->getPointer();
        iovecs[i].iov_len = buffer->size();
    }
    iovecs_vector.emplace_back(iovecs);
    int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
    if (ret != 0)
    {
        throw InvalidArgumentException(
            "DirectUringRandomAccessFile::RegisterMoreBuffer: register buffer "
            "fails. ");
    }
    return true;
}

DirectUringRandomAccessFile::~DirectUringRandomAccessFile()
{
}

std::shared_ptr<ByteBuffer> DirectUringRandomAccessFile::readAsync(
    int length, std::shared_ptr<ByteBuffer> buffer, int index, int ring_index,
    int start_offset)
{
    auto ring = DirectUringRandomAccessFile::getRing(ring_index);
    auto offset = start_offset;

    if (enableDirect)
    {
        struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
        if (!sqe)
        {
            throw std::runtime_error("DirectUringRandomAccessFile::readAsync: failed "
                "to get SQE, submission queue is full");
        }
        // the file will be read from blockStart(fileOffset), and the first
        // fileDelta bytes should be ignored.
        uint64_t fileOffsetAligned = directIoLib->blockStart(offset);
        uint64_t toRead = directIoLib->blockEnd(offset + length) -
            directIoLib->blockStart(offset);
        size_t block_size = directIoLib->getBlockSize(); // 假设存在获取块大小的方法
        size_t required_buffer_size = (offset - fileOffsetAligned) + length;
        if (buffer->size() < required_buffer_size)
        {
            std::stringstream ss;
            std::cout << "DirectUringRandomAccessFile::readAsync: buffer size "
                "insufficient. "
                << "Required: " << required_buffer_size
                << ", Actual: " << buffer->size()
                << ", ring_index: " << ring_index << ", index: " << index
                << "Required alignment: " << block_size << ", length:" << length
                << std::endl;;
            throw InvalidArgumentException(ss.str());
        }

        io_uring_prep_read_fixed(sqe, fd, buffer->getPointer(), toRead,
                                 fileOffsetAligned, index);
        if (fd < 0)
        {
            throw std::runtime_error(
                "DirectUringRandomAccessFile::readAsync: invalid file descriptor");
        }

        auto bb = std::make_shared<ByteBuffer>(*buffer, offset - fileOffsetAligned,
                                               length);
        seekByIndex(offset + length, ring_index);
        return bb;
    }
    else
    {
        struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
        io_uring_prep_read_fixed(sqe, fd, buffer->getPointer(), length, offset,
                                 index);
        seek(offset + length);
        auto result = std::make_shared<ByteBuffer>(*buffer, 0, length);
        return result;
    }
}

void DirectUringRandomAccessFile::seekByIndex(long off, int index)
{
    if (index < 0 || static_cast<size_t>(index) >= offsets_vector.size())
    {
        std::stringstream ss;
        ss << "DirectUringRandomAccessFile::seekByIndex: invalid index. "
            << "Index: " << index << ", Vector size: " << offsets_vector.size();
        throw InvalidArgumentException(ss.str());
    }
    if (off < 0)
    {
        std::stringstream ss;
        ss << "DirectUringRandomAccessFile::seekByIndex: invalid offset. "
            << "Offset: " << off << ", Index: " << index;
        throw InvalidArgumentException(ss.str());
    }
    offsets_vector.at(index) = off;
}

void DirectUringRandomAccessFile::readAsyncSubmit(
    std::unordered_map<int, uint32_t> sizes,
    std::unordered_set<int> ring_index)
{
    for (auto i : ring_index)
    {
        auto ring = DirectUringRandomAccessFile::getRing(i);
        int ret = io_uring_submit(ring);
        if (ret != sizes[i])
        {
            std::string error_msg =
                "DirectUringRandomAccessFile::readAsyncSubmit: submit fails. "
                "Index: " +
                std::to_string(i) +
                ", "
                "Expected size: " +
                std::to_string(sizes[i]) +
                ", "
                "Actual return value: " +
                std::to_string(ret);
            throw InvalidArgumentException(error_msg);
        }
    }
}

void DirectUringRandomAccessFile::readAsyncComplete(
    std::unordered_map<int, uint32_t> sizes,
    std::unordered_set<int> ring_index)
{
    // Important! We cannot write the code as io_uring_wait_cqe_nr(ring, &cqe,
    // iovecSize). The reason is unclear, but some random bugs would happen. It
    // takes me nearly a week to find this bug
    for (auto idx : ring_index)
    {
        // 重命名外层循环变量，避免与内层冲突
        struct io_uring_cqe* cqe;
        auto ring = DirectUringRandomAccessFile::getRing(idx);
        for (int j = 0; j < sizes[idx]; j++) // 注意：这里使用外层的idx作为sizes的键
        {
            if (io_uring_wait_cqe_nr(ring, &cqe, 1) != 0)
            {
                throw InvalidArgumentException(
                    "DirectUringRandomAccessFile::readAsyncComplete: wait cqe fails");
            }
            io_uring_cqe_seen(ring, cqe);
        }
    }
}

struct io_uring* DirectUringRandomAccessFile::getRing(int index)
{
    if (index >= ring_vector.size())
    {
        // need to add more rings
        // initialize io_uring ring
        struct io_uring* ring = nullptr;
        auto flag = std::stoi(
            ConfigFactory::Instance().getProperty("pixels.io_uring.mode"));
        // get io_uring flags for io mode
        // 0 interrupt-dirven
        // 1 IORING_SETUP_IOPOLL
        // 2 IORING_SETUP_SQPOLL
        if (ring == nullptr)
        {
            ring = new io_uring();
            if (io_uring_queue_init(4096, ring, flag) < 0)
            {
                throw InvalidArgumentException(
                    "DirectRandomAccessFile: initialize io_uring fails.");
            }
        }
        ring_vector.emplace_back(ring);
        offsets_vector.emplace_back(0);
    }
    return ring_vector[index];
}
