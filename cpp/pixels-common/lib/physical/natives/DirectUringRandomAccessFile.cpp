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

thread_local struct io_uring *DirectUringRandomAccessFile::ring = nullptr;
thread_local bool DirectUringRandomAccessFile::isRegistered = false;
thread_local struct iovec *DirectUringRandomAccessFile::iovecs = nullptr;
thread_local uint32_t
DirectUringRandomAccessFile::iovecSize = 0;
thread_local std::map<uint32_t, uint64_t>
    DirectUringRandomAccessFile::registeredColIds;

DirectUringRandomAccessFile::DirectUringRandomAccessFile(const std::string &file) : DirectRandomAccessFile(file)
{

}

void DirectUringRandomAccessFile::RegisterBufferFromPool(std::vector <uint32_t> colIds)
{
  std::vector <std::shared_ptr<ByteBuffer>> tmpBuffers;
//  for(auto colId:colIds) {
//      if(DirectUringRandomAccessFile::registeredColIds.find(colId)==DirectUringRandomAccessFile::registeredColIds.end()){
//        for(auto buffer: ::BufferPool::buffers){
//          tmpBuffers.emplace_back(buffer[colId]);
//        }
//        iovecs = (iovec *) calloc(tmpBuffers.size(), sizeof(struct iovec));
//        iovecSize = tmpBuffers.size();
//        for (auto i = 0; i < tmpBuffers.size(); i++)
//        {
//          auto buffer = tmpBuffers.at(i);
//          iovecs[i].iov_base = buffer->getPointer();
//          iovecs[i].iov_len = buffer->size();
//          memset(iovecs[i].iov_base, 0, buffer->size());
//        }
//        int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
//        if (ret != 0)
//        {
//          throw InvalidArgumentException("DirectUringRandomAccessFile::RegisterBuffer: register buffer fails. ");
//        }
//        DirectUringRandomAccessFile::registeredColIds[colId]=1;
//      }
//    }


    if (!isRegistered)
    {
        for (auto buffer: ::BufferPool::buffers)
        {
            for (auto colId: colIds)
            {
                tmpBuffers.emplace_back(buffer[colId]);
            }
        }
        iovecs = (iovec *) calloc(tmpBuffers.size(), sizeof(struct iovec));
        iovecSize = tmpBuffers.size();
        for (auto i = 0; i < tmpBuffers.size(); i++)
        {
            auto buffer = tmpBuffers.at(i);
            iovecs[i].iov_base = buffer->getPointer();
            iovecs[i].iov_len = buffer->size();
            memset(iovecs[i].iov_base, 0, buffer->size());
        }
        int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
        if (ret != 0)
        {
            throw InvalidArgumentException("DirectUringRandomAccessFile::RegisterBuffer: register buffer fails. ");
        }
        isRegistered = true;
    }
}


void DirectUringRandomAccessFile::RegisterBuffer(std::vector <std::shared_ptr<ByteBuffer>> buffers)
{
    if (!isRegistered)
    {
        iovecs = (iovec *) calloc(buffers.size(), sizeof(struct iovec));
        iovecSize = buffers.size();
        for (auto i = 0; i < buffers.size(); i++)
        {
            auto buffer = buffers.at(i);
            iovecs[i].iov_base = buffer->getPointer();
            iovecs[i].iov_len = buffer->size();
            memset(iovecs[i].iov_base, 0, buffer->size());
        }
        int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
        if (ret != 0)
        {
            throw InvalidArgumentException("DirectUringRandomAccessFile::RegisterBuffer: register buffer fails. ");
        }
        isRegistered = true;
    }
}

void DirectUringRandomAccessFile::Initialize()
{
    // initialize io_uring ring
    if (ring == nullptr)
    {
        ring = new io_uring();
        if (io_uring_queue_init(4096, ring, 0) < 0)
        {
            throw InvalidArgumentException("DirectRandomAccessFile: initialize io_uring fails.");
        }
    }
}

void DirectUringRandomAccessFile::Reset()
{
    // Important! Because sometimes ring is nullptr here.
    // For example, two threads A and B share the same global state. If A finish all files while B just starts,
    // B would execute Reset function from InitLocal. If we don't set this 'if' branch, ring would be double freed.
    if (ring != nullptr)
    {
        // We don't use this function anymore since it slows down the speed
        //		if(io_uring_unregister_buffers(ring) != 0) {
        //			throw InvalidArgumentException("DirectUringRandomAccessFile::UnregisterBuffer: unregister buffer fails. ");
        //		}
        io_uring_queue_exit(ring);
        delete (ring);
        ring = nullptr;
        isRegistered = false;
    }
    if (iovecs != nullptr)
    {
        free(iovecs);
        iovecs = nullptr;
    }
}

DirectUringRandomAccessFile::~DirectUringRandomAccessFile()
{

}

std::shared_ptr <ByteBuffer>
DirectUringRandomAccessFile::readAsync(int length, std::shared_ptr <ByteBuffer> buffer, int index)
{
    if (enableDirect)
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
//		if(length > iovecs[index].iov_len) {
//			throw InvalidArgumentException("DirectUringRandomAccessFile::readAsync: the length is larger than buffer length.");
//		}
        // the file will be read from blockStart(fileOffset), and the first fileDelta bytes should be ignored.
        uint64_t fileOffsetAligned = directIoLib->blockStart(offset);
        uint64_t toRead = directIoLib->blockEnd(offset + length) - directIoLib->blockStart(offset);
        io_uring_prep_read_fixed(sqe, fd, buffer->getPointer(), toRead,
                                 fileOffsetAligned, index);
        auto bb = std::make_shared<ByteBuffer>(*buffer,
                                               offset - fileOffsetAligned, length);
        seek(offset + length);
        return bb;
    }
    else
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
//		if(length > iovecs[index].iov_len) {
//			throw InvalidArgumentException("DirectUringRandomAccessFile::readAsync: the length is larger than buffer length.");
//		}
        io_uring_prep_read_fixed(sqe, fd, buffer->getPointer(), length, offset, index);
        seek(offset + length);
        auto result = std::make_shared<ByteBuffer>(*buffer, 0, length);
        return result;
    }

}


void DirectUringRandomAccessFile::readAsyncSubmit(int size)
{
    int ret = io_uring_submit(ring);
    if (ret != size)
    {
        throw InvalidArgumentException("DirectUringRandomAccessFile::readAsyncSubmit: submit fails");
    }
}

void DirectUringRandomAccessFile::readAsyncComplete(int size)
{
    // Important! We cannot write the code as io_uring_wait_cqe_nr(ring, &cqe, iovecSize).
    // The reason is unclear, but some random bugs would happen. It takes me nearly a week to find this bug
    struct io_uring_cqe *cqe;
    for (int i = 0; i < size; i++)
    {
        if (io_uring_wait_cqe_nr(ring, &cqe, 1) != 0)
        {
            throw InvalidArgumentException("DirectUringRandomAccessFile::readAsyncComplete: wait cqe fails");
        }
        io_uring_cqe_seen(ring, cqe);
    }
}


