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
 * @create 2023-04-19
 */
#include "physical/natives/DirectIoLib.h"
#include <sys/mman.h>  // mmap, munmap, madvise
#include <new>         // 对于std::bad_alloc
#include <cstring>     // 对于strerror
#include <errno.h>     // 对于errno


DirectIoLib::DirectIoLib(int fsBlockSize)
{
    this->fsBlockSize = fsBlockSize;
    this->fsBlockNotMask = ~((long) fsBlockSize - 1);
}

template <typename T> struct huge_page_allocator {
  constexpr static std::size_t huge_page_size = 1 << 21; // 2 MiB
  using value_type = T;

  huge_page_allocator() = default;
  template <class U>
  constexpr huge_page_allocator(const huge_page_allocator<U> &) noexcept {}

  size_t round_to_huge_page_size(size_t n) {
    return (((n - 1) / huge_page_size) + 1) * huge_page_size;
  }

  T *allocate(std::size_t n) {
    if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
      throw std::bad_alloc();
    }
    auto p = static_cast<T *>(mmap(
        nullptr, round_to_huge_page_size(n * sizeof(T)), PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0));
    if (p == MAP_FAILED) {
      throw std::bad_alloc();
    }
    return p;
  }

  void deallocate(T *p, std::size_t n) {
    munmap(p, round_to_huge_page_size(n * sizeof(T)));
  }
};

std::shared_ptr<ByteBuffer> DirectIoLib::allocateDirectBuffer(long size, bool isSmallBuffer)
{
    auto strToBool = [](const std::string& s) {
        return s == "true" || s == "1" || s == "yes";
    };

    std::string configValue = ConfigFactory::Instance().getProperty("pixel.bufferpool.hugepage");
    bool isHugePage = strToBool(configValue);

    if (isHugePage && !isSmallBuffer) {
        // 使用大页分配器进行内存分配
        try {
            // 计算需要分配的大小（考虑块对齐）
            int toAllocate = blockEnd(size) + (size == 1 ? 0 : fsBlockSize);

            // 创建大页分配器实例
            huge_page_allocator<uint8_t> allocator;

            // 分配内存
            uint8_t* directBufferPointer = allocator.allocate(toAllocate);

            // 创建自定义删除器，使用分配器释放内存
            auto deleter = [allocator, toAllocate](ByteBuffer* buf) mutable {
                if (buf && buf->getBuffer()) {
                    // 使用分配器的deallocate方法释放内存
                    allocator.deallocate(const_cast<uint8_t*>(buf->getBuffer()), toAllocate);
                }
                // delete buf; // 释放ByteBuffer对象本身
            };

            // 创建ByteBuffer（不接管内存所有权，由shared_ptr的删除器管理）
            ByteBuffer* buf = new ByteBuffer(directBufferPointer, toAllocate, false);

            // 返回带有自定义删除器的shared_ptr
            return std::shared_ptr<ByteBuffer>(buf, deleter);
        }
        catch (const std::bad_alloc& e) {
            throw std::runtime_error("大页内存分配失败: " + std::string(e.what()) +
                                   "，错误码: " + std::string(strerror(errno)));
        }
    } else {
        // 普通内存分配路径
        int toAllocate = blockEnd(size) + (size == 1 ? 0 : fsBlockSize);
        uint8_t* directBufferPointer;

        // 使用posix_memalign进行对齐分配
        if (posix_memalign((void**)&directBufferPointer, fsBlockSize, toAllocate) != 0) {
            throw std::runtime_error("普通内存分配失败: " + std::string(strerror(errno)));
        }

        // 创建自定义删除器释放内存
        auto deleter = [](ByteBuffer* buf) {
            if (buf && buf->getBuffer()) {
                free(const_cast<uint8_t*>(buf->getBuffer()));
            }
            // delete buf;
        };

        // 创建ByteBuffer并返回shared_ptr
        ByteBuffer* buf = new ByteBuffer(directBufferPointer, toAllocate, false);
        return std::shared_ptr<ByteBuffer>(buf, deleter);
    }
}
//
// std::shared_ptr <ByteBuffer> DirectIoLib::allocateDirectBuffer(long size,bool isSmallBuffer)
// {
// auto strToBool = [](const std::string& s) {
//         return s == "true" || s == "1" || s == "yes";
//     };
//     std::string configValue = ConfigFactory::Instance().getProperty("pixel.bufferpool.hugepage");
//     bool isHugePage = strToBool(configValue);
//     if (isHugePage&&!isSmallBuffer) {
//         // 计算需要分配的大页数量，向上取整
//         // size_t hugePagesNeeded = (size + HUGE_PAGE_SIZE - 1) / HUGE_PAGE_SIZE;
//         // size_t toAllocate = hugePagesNeeded * HUGE_PAGE_SIZE;
//         //
//         // // 使用大页分配内存
//         // uint8_t *directBufferPointer = static_cast<uint8_t*>(
//         //     mmap(nullptr, toAllocate, PROT_READ | PROT_WRITE,
//         //          MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0)
//         // );
//         //
//         // // 检查分配是否成功
//         // if (directBufferPointer == MAP_FAILED) {
//         //     throw std::runtime_error("大页内存分配失败: " + std::string(strerror(errno)));
//         // }
//         //
//         // // 创建ByteBuffer并使用自定义删除器
//         // auto deleter = [toAllocate](ByteBuffer* buf) {
//         //     if (buf && buf->getBuffer()) {
//         //         munmap(buf->getBuffer(), toAllocate);  // 使用munmap释放大页内存
//         //     }
//         // };
//         //
//         // // 创建ByteBuffer对象（不接管内存所有权）
//         // ByteBuffer* buf = new ByteBuffer(directBufferPointer, toAllocate, false);
//         //
//         // // 创建shared_ptr并指定自定义删除器
//         // return std::shared_ptr<ByteBuffer>(buf, deleter);
//         int toAllocate = blockEnd(size) + (size == 1 ? 0 : fsBlockSize);
//         uint8_t *directBufferPointer;
//         posix_memalign((void **) &directBufferPointer, fsBlockSize, toAllocate);
//         madvise(directBufferPointer, toAllocate, MADV_HUGEPAGE);
//         auto directBuffer = std::make_shared<ByteBuffer>(directBufferPointer, toAllocate, false);
//         return directBuffer;
//     }else {
//         int toAllocate = blockEnd(size) + (size == 1 ? 0 : fsBlockSize);
//         uint8_t *directBufferPointer;
//         posix_memalign((void **) &directBufferPointer, fsBlockSize, toAllocate);
//         auto directBuffer = std::make_shared<ByteBuffer>(directBufferPointer, toAllocate, false);
//         return directBuffer;
//     }
// }

int DirectIoLib::getToAllocate(int size) {
    return blockEnd(size) + (size == 1 ? 0 : fsBlockSize);
}




std::shared_ptr <ByteBuffer> DirectIoLib::read(int fd, long fileOffset,
                                               std::shared_ptr <ByteBuffer> directBuffer, long length)
{
    // the file will be read from blockStart(fileOffset), and the first fileDelta bytes should be ignored.
    long fileOffsetAligned = blockStart(fileOffset);
    long toRead = blockEnd(fileOffset + length) - blockStart(fileOffset);
    ssize_t ret = pread(fd, directBuffer->getPointer(), toRead, fileOffsetAligned);
    if (ret == -1) {
        perror("pread failed");
        throw InvalidArgumentException("DirectIoLib::read: pread fail. ");
    }
    auto bb = std::make_shared<ByteBuffer>(*directBuffer,
                                           fileOffset - fileOffsetAligned, length);
    return bb;
}


long DirectIoLib::blockStart(long value)
{
    return (value & fsBlockNotMask);
}

long DirectIoLib::blockEnd(long value)
{
    return (value + fsBlockSize - 1) & fsBlockNotMask;
}

