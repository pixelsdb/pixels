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
thread_local  std::vector<struct io_uring*> DirectUringRandomAccessFile::ring_vector;
thread_local  std::vector<struct iovec*> DirectUringRandomAccessFile::iovecs_vector;
thread_local  std::vector<long> DirectUringRandomAccessFile::offsets_vector;
thread_local  uint32_t DirectUringRandomAccessFile::iovecSize = 0;


// static thread_local int memset_count = 0;
// static thread_local  std::chrono::nanoseconds total_time0(0);  // 总耗时（纳秒）
// static thread_local  std::chrono::nanoseconds total_time(0);  // memest总耗时（纳秒）

DirectUringRandomAccessFile::DirectUringRandomAccessFile(const std::string &file) : DirectRandomAccessFile(file)
{

}

void DirectUringRandomAccessFile::RegisterBufferFromPool(std::vector <uint32_t> colIds)
{
    // std::cout << "函数:RegisterBufferFromPool 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    // std::lock_guard<std::mutex> lock(mutex_);
    if (!isRegistered) {

        std::vector <std::shared_ptr<ByteBuffer>> tmpBuffers;
        auto ring=DirectUringRandomAccessFile::getRing(0);
        struct iovec* iovecs=nullptr;
        for (auto buffers : ::BufferPool::registeredBuffers){
            for (auto buffer:buffers) {
                buffer->setIsRegistered(true);
                tmpBuffers.emplace_back(buffer->getBuffer());
            }
        }
        // auto start0 = std::chrono::high_resolution_clock::now();
        iovecs = (iovec *) calloc(tmpBuffers.size(), sizeof(struct iovec));
        iovecSize = tmpBuffers.size();
        for (auto i = 0; i < tmpBuffers.size(); i++)
        {
            auto buffer = tmpBuffers.at(i);
            iovecs[i].iov_base = buffer->getPointer();
            iovecs[i].iov_len = buffer->size();
            // auto start = std::chrono::high_resolution_clock::now();

            // 目标代码
            // memset(iovecs[i].iov_base, 0, buffer->size());

            // 记录结束时间，累加耗时
            // auto end = std::chrono::high_resolution_clock::now();
            // total_time += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            // total_time0+=std::chrono::duration_cast<std::chrono::nanoseconds>(end-start0);
            // 累加次数
            // memset_count++;
        }
        // std::cout << "memset执行次数：" << memset_count << std::endl;
        // 转换为易读单位（如微秒/毫秒）
        // auto total_us0 = std::chrono::duration_cast<std::chrono::microseconds>(total_time0).count();
        // auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(total_time).count();
        // std::cout << "总用时：" << total_us0 << " 微秒" << std::endl;
        // std::cout << "memset用时：" << total_us << " 微秒" << std::endl;
        iovecs_vector.emplace_back(iovecs);
        int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
        // std::cout<<"ret:"<<ret<<" iovecSize:"<<iovecSize<<std::endl;
        if (ret != 0)
        {
            std::cerr << "io_uring_register_buffers failed: " << strerror(errno) << ", ret=" << ret <<std::strerror(ret)<< std::endl;
            throw InvalidArgumentException("DirectUringRandomAccessFile::RegisterBufferFromPool: register buffer fails. ");
        }
        isRegistered = true;
    }
    // std::cout << "函数:RegisterBufferFromPool 线程 " << std::this_thread::get_id() << " 即将释放global_mutex" << std::endl;
}


void DirectUringRandomAccessFile::RegisterBuffer(std::vector <std::shared_ptr<ByteBuffer>> buffers)
{

    // std::cout << "函数:RegisterBuffer 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    if (!isRegistered)
    {
        auto ring=DirectUringRandomAccessFile::getRing(0);
        struct iovec* iovecs=nullptr;
        iovecs = (iovec *) calloc(buffers.size(), sizeof(struct iovec));
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
            throw InvalidArgumentException("DirectUringRandomAccessFile::RegisterBuffer: register buffer fails. ");
        }
        isRegistered = true;
    }
    // std::cout << "函数:RegisterBuffer 线程 " << std::this_thread::get_id() << " 释放global_mutex" << std::endl;
}

void DirectUringRandomAccessFile::Initialize()
{
    std::lock_guard<std::mutex> lock(mutex_);
    // std::cout << "函数:Initialize 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    getRing(0);
    // std::cout << "函数:Initialze 线程 " << std::this_thread::get_id() << " 成功释放global_mutex" << std::endl;

    // // initialize io_uring ring
    // struct io_uring *ring=nullptr;
    // if (getRing(0) == nullptr)
    // {
    //     ring = new io_uring();
    //     if (io_uring_queue_init(4096, ring, 0) < 0)
    //     {
    //         throw InvalidArgumentException("DirectRandomAccessFile: initialize io_uring fails.");
    //     }
    // }
    // ring_vector.emplace_back(ring);
    // offsets_vector.emplace_back(0);
}

void DirectUringRandomAccessFile::Reset()
{
    // std::cout << "函数:Reset 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    // Important! Because sometimes ring is nullptr here.
    // For example, two threads A and B share the same global state. If A finish all files while B just starts,
    // B would execute Reset function from InitLocal. If we don't set this 'if' branch, ring would be double freed.
    for (auto ring :ring_vector) {
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
    }
    ring_vector.clear();
    for (auto iovecs:iovecs_vector) {
        if (iovecs != nullptr)
        {
            free(iovecs);
            iovecs = nullptr;
        }
    }
    iovecs_vector.clear();
    std::cout << "函数:Reset 线程 " << std::this_thread::get_id() << " 成功释放global_mutex" << std::endl;
}

bool DirectUringRandomAccessFile::RegisterMoreBuffer(int index,std::vector<std::shared_ptr<ByteBuffer>> buffers) {
    // std::lock_guard<std::mutex> lock(mutex_);
    std::cout << "函数:RegisterMoreBuffer 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    assert(isRegistered);
    std::cout<<"现在有"<<ring_vector.size()<<"个ring"<<std::endl;
    auto ring=DirectUringRandomAccessFile::getRing(index);
    struct iovec* iovecs=nullptr;
    iovecs = (iovec *) calloc(buffers.size(), sizeof(struct iovec));
    iovecSize = buffers.size();
    for (auto i = 0; i < buffers.size(); i++)
    {
        auto buffer = buffers.at(i);
        iovecs[i].iov_base = buffer->getPointer();
        iovecs[i].iov_len = buffer->size();
        // memset(iovecs[i].iov_base, 0, buffer->size());
    }
    iovecs_vector.emplace_back(iovecs);
    int ret = io_uring_register_buffers(ring, iovecs, iovecSize);
    if (ret != 0)
    {
        throw InvalidArgumentException("DirectUringRandomAccessFile::RegisterMoreBuffer: register buffer fails. ");
    }
    // std::cout << "函数:RegisterMoreBuffer 线程 " << std::this_thread::get_id() << " 成功释放global_mutex" << std::endl;
    return true;

}

DirectUringRandomAccessFile::~DirectUringRandomAccessFile()
{

}

std::shared_ptr <ByteBuffer>
DirectUringRandomAccessFile::readAsync(int length, std::shared_ptr <ByteBuffer> buffer, int index,int ring_index,int start_offset)
{
    // std::cout << "函数:readAsync 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    // std::lock_guard<std::mutex> lock(mutex_);
    auto ring=DirectUringRandomAccessFile::getRing(ring_index);
    auto offset = start_offset;

    if (enableDirect)
    {
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            throw std::runtime_error("DirectUringRandomAccessFile::readAsync: failed to get SQE, submission queue is full");
        }
//		if(length > iovecs[index].iov_len) {
//			throw InvalidArgumentException("DirectUringRandomAccessFile::readAsync: the length is larger than buffer length.");
//		}
        // the file will be read from blockStart(fileOffset), and the first fileDelta bytes should be ignored.
        uint64_t fileOffsetAligned = directIoLib->blockStart(offset);
        uint64_t toRead = directIoLib->blockEnd(offset + length) - directIoLib->blockStart(offset);
        // 检查缓冲区大小是否足够
        size_t block_size = directIoLib->getBlockSize(); // 假设存在获取块大小的方法
        size_t required_buffer_size = (offset - fileOffsetAligned) + length;
        if (buffer->size() < required_buffer_size) {
            std::stringstream ss;
            std::cout << "DirectUringRandomAccessFile::readAsync: buffer size insufficient. "
               << "Required: " << required_buffer_size << ", Actual: " << buffer->size()
               << ", ring_index: " << ring_index << ", index: " << index
            << "Required alignment: " << block_size
            <<", length:"<<length<<std::endl;;
            throw InvalidArgumentException(ss.str());
        }

        // 检查缓冲区对齐是否符合直接I/O要求
        uintptr_t buffer_addr = reinterpret_cast<uintptr_t>(buffer->getPointer());
        if (buffer_addr % block_size != 0) {
            std::stringstream ss;
            ss << "DirectUringRandomAccessFile::readAsync: buffer misaligned. "
               << "Required alignment: " << block_size << " bytes, "
               << "Actual address: 0x" << std::hex << buffer_addr
               << ", ring_index: " << ring_index << ", index: " << index
                <<", length:"<<length<<std::endl;
            throw InvalidArgumentException(ss.str());
        }
        io_uring_prep_read_fixed(sqe, fd, buffer->getPointer(), toRead,
                                 fileOffsetAligned, index);
        if (fd < 0) {
            throw std::runtime_error("DirectUringRandomAccessFile::readAsync: invalid file descriptor");
        }
        // if (buffer->size()<offset-fileOffsetAligned+length) {
        //     std::cout<<"Here has been error!"<<std::endl;
        //     std::cout<<"fileOffsetAligned:"<<fileOffsetAligned<<" toRead:"<<toRead<<" offset:"<<offset<<" buffer:"<<
        //     static_cast<void*>(buffer->getPointer())<<" index:"<<index<<" ring_index: "<<ring_index<<" length: "<<length<<
        //         " buffer size:"<<buffer->size()<<" startID:"<<offset - fileOffsetAligned<<std::endl;
        // }
        auto bb = std::make_shared<ByteBuffer>(*buffer,
                                               offset - fileOffsetAligned, length);
        seekByIndex(offset + length,ring_index);
        // if (ring_index!=0) {
        //     // std::cout<<"notice uring File index:"<<ring_index<<" buffer:"<<static_cast<void*>(buffer->getPointer())<<" start_offset"<<start_offset<<std::endl;
        //     std::cout<<"fileOffsetAligned:"<<fileOffsetAligned<<" toRead:"<<toRead<<" offset:"<<offset<<" buffer:"<<
        //     static_cast<void*>(buffer->getPointer())<<" index:"<<index<<" ring_index: "<<ring_index<<" length: "<<length<<" bb:"<<static_cast<void*>(bb->getPointer())<<
        //         " buffer size:"<<buffer->size()<<" startID:"<<offset - fileOffsetAligned<<std::endl;
        // }
        // std::cout<<"fileOffsetAligned:"<<fileOffsetAligned<<" toRead:"<<toRead<<" offset:"<<offset<<" buffer:"<<
        // static_cast<void*>(buffer->getPointer())<<" index:"<<index<<" ring_index: "<<ring_index<<" length: "<<length<<" bb:"<<static_cast<void*>(bb->getPointer())<<std::endl;
        // std::cout << "函数:readAsync 线程 " << std::this_thread::get_id() << " 成功释放global_mutex" << std::endl;
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
void DirectUringRandomAccessFile::seekByIndex(long off,int index) {
    // 检查索引有效性
    if (index < 0 || static_cast<size_t>(index) >= offsets_vector.size()) {
        std::stringstream ss;
        ss << "DirectUringRandomAccessFile::seekByIndex: invalid index. "
           << "Index: " << index << ", Vector size: " << offsets_vector.size();
        throw InvalidArgumentException(ss.str());
    }
    // 检查偏移量有效性（假设偏移量不能为负）
    if (off < 0) {
        std::stringstream ss;
        ss << "DirectUringRandomAccessFile::seekByIndex: invalid offset. "
           << "Offset: " << off << ", Index: " << index;
        throw InvalidArgumentException(ss.str());
    }
    offsets_vector.at(index)=off;
}



void DirectUringRandomAccessFile::readAsyncSubmit(std::unordered_map<int,uint32_t> sizes,std::unordered_set<int> ring_index)
{
    // std::lock_guard<std::mutex> lock(mutex_);
    // std::cout << "函数:readAsyncSubmit 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    for (auto i:ring_index) {
        auto ring=DirectUringRandomAccessFile::getRing(i);
        int ret = io_uring_submit(ring);
        // std::cout<<"线程:"<<std::this_thread::get_id()<<" ret:"<<ret<<" i:"<<i<<std::endl;
        if (ret != sizes[i])
        {
            // 构造包含详细信息的错误消息
            std::string error_msg = "DirectUringRandomAccessFile::readAsyncSubmit: submit fails. "
                                    "Index: " + std::to_string(i) + ", "
                                    "Expected size: " + std::to_string(sizes[i]) + ", "
                                    "Actual return value: " + std::to_string(ret);
            throw InvalidArgumentException(error_msg);
        }
    }
    // std::cout << "函数:readAsyncSubmit 线程 " << std::this_thread::get_id() << " 成功释放global_mutex" << std::endl;
}

void DirectUringRandomAccessFile::readAsyncComplete(std::unordered_map<int,uint32_t> sizes,std::unordered_set<int> ring_index)
{
    // std::lock_guard<std::mutex> lock(mutex_);
    // std::cout << "函数:readAsyncComplete 线程 " << std::this_thread::get_id() << " 成功获取global_mutex" << std::endl;
    // Important! We cannot write the code as io_uring_wait_cqe_nr(ring, &cqe, iovecSize).
    // The reason is unclear, but some random bugs would happen. It takes me nearly a week to find this bug
    for (auto idx : ring_index) {  // 重命名外层循环变量，避免与内层冲突
        struct io_uring_cqe *cqe;
        auto ring = DirectUringRandomAccessFile::getRing(idx);

        // 为当前ring设置超时时间（根据实际场景调整，这里设为5秒）
        struct __kernel_timespec timeout = {
            .tv_sec = 1,    // 秒
            .tv_nsec = 0    // 纳秒
        };

        // 内层循环使用j作为变量，避免与外层idx冲突
        for (int j = 0; j < sizes[idx]; j++)  // 注意：这里使用外层的idx作为sizes的键
        {

            if (io_uring_wait_cqe_nr(ring, &cqe, 1) != 0)
            {
                throw InvalidArgumentException("DirectUringRandomAccessFile::readAsyncComplete: wait cqe fails");
            }
            io_uring_cqe_seen(ring, cqe);
            // 使用带超时的等待函数，替代原有的io_uring_wait_cqe_nr
            // int ret = io_uring_wait_cqe_timeout(ring, &cqe, &timeout);
            // if (ret != 0) {
            //     if (ret == -ETIME) {
            //         // 超时错误：明确抛出超时异常，便于上层处理
            //         throw InvalidArgumentException(
            //             "DirectUringRandomAccessFile::readAsyncComplete: "
            //             "wait cqe timeout for ring index " + std::to_string(idx) +
            //             " after " + std::to_string(timeout.tv_sec) + " seconds"
            //         );
            //     } else if (ret == -EINTR) {
            //         // 信号中断：可根据需求选择重试或抛出异常
            //         throw InvalidArgumentException(
            //             "DirectUringRandomAccessFile::readAsyncComplete: "
            //             "wait cqe interrupted by signal for ring index " + std::to_string(idx)
            //         );
            //     } else {
            //         // 其他错误（如参数错误、ring已关闭等）
            //         throw InvalidArgumentException(
            //             "DirectUringRandomAccessFile::readAsyncComplete: "
            //             "wait cqe fails with error " + std::to_string(ret) +
            //             " for ring index " + std::to_string(idx)
            //         );
            //     }
            // }
            //
            // // 检查CQE结果是否正常（可选，根据业务需求）
            // if (cqe->res < 0) {
            //     throw InvalidArgumentException(
            //         "DirectUringRandomAccessFile::readAsyncComplete: "
            //         "I/O operation failed with error " + std::to_string(cqe->res) +
            //         " for ring index " + std::to_string(idx)
            //     );
            // }
            //
            // io_uring_cqe_seen(ring, cqe);
        }
    }
    // std::cout << "函数:readAsyncComplete 线程 " << std::this_thread::get_id() << " 成功释放global_mutex" << std::endl;
}

struct io_uring *DirectUringRandomAccessFile::getRing(int index) {
    if (index>=ring_vector.size()) {
        // need to add more rings
        // initialize io_uring ring
        struct io_uring *ring=nullptr;
        if (ring == nullptr)
        {
            ring = new io_uring();
            if (io_uring_queue_init(4096, ring, 0) < 0)
            {
                throw InvalidArgumentException("DirectRandomAccessFile: initialize io_uring fails.");
            }
        }
        ring_vector.emplace_back(ring);
        offsets_vector.emplace_back(0);
    }
       return ring_vector[index];
}

