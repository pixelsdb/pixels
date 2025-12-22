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
#include <new>
#include <cstring>
#include <errno.h>

DirectIoLib::DirectIoLib(int fsBlockSize)
{
    this->fsBlockSize = fsBlockSize;
    this->fsBlockNotMask = ~((long)fsBlockSize - 1);
}

template <typename T>
struct huge_page_allocator
{
    constexpr static std::size_t huge_page_size = 1 << 21; // 2 MiB
    using value_type = T;

    huge_page_allocator() = default;

    template <class U>
    constexpr huge_page_allocator(const huge_page_allocator<U>&) noexcept
    {
    }

    size_t round_to_huge_page_size(size_t n)
    {
        return (((n - 1) / huge_page_size) + 1) * huge_page_size;
    }

    T* allocate(std::size_t n)
    {
        if (n > std::numeric_limits<std::size_t>::max() / sizeof(T))
        {
            throw std::bad_alloc();
        }
        auto p = static_cast<T*>(mmap(
            nullptr, round_to_huge_page_size(n * sizeof(T)), PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0));
        if (p == MAP_FAILED)
        {
            throw std::bad_alloc();
        }
        return p;
    }

    void deallocate(T* p, std::size_t n)
    {
        munmap(p, round_to_huge_page_size(n * sizeof(T)));
    }
};

std::shared_ptr<ByteBuffer> DirectIoLib::allocateDirectBuffer(long size, bool isSmallBuffer)
{
    // Lambda function to convert string to boolean
    auto strToBool = [](const std::string& s)
    {
        return s == "true" || s == "1" || s == "yes";
    };

    // Get configuration value for huge page buffer pool
    std::string configValue = ConfigFactory::Instance().getProperty("pixel.bufferpool.hugepage");
    bool isHugePage = strToBool(configValue);

    if (isHugePage && !isSmallBuffer)
    {
        // Allocate memory using huge page allocator
        try
        {
            // Calculate the size to allocate (considering block alignment)
            int toAllocate = blockEnd(size) + (size == 1 ? 0 : fsBlockSize);

            // Create huge page allocator instance
            huge_page_allocator<uint8_t> allocator;

            // Allocate memory
            uint8_t* directBufferPointer = allocator.allocate(toAllocate);

            // Create custom deleter to free memory using the allocator
            auto deleter = [allocator, toAllocate](ByteBuffer* buf) mutable
            {
                if (buf && buf->getBuffer())
                {
                    // Deallocate memory using the allocator's deallocate method
                    allocator.deallocate(const_cast<uint8_t*>(buf->getBuffer()), toAllocate);
                }
                // delete buf; // Free the ByteBuffer object itself
            };

            // Create ByteBuffer (does not take over memory ownership, managed by shared_ptr's deleter)
            ByteBuffer* buf = new ByteBuffer(directBufferPointer, toAllocate, false);

            // Return shared_ptr with custom deleter
            return std::shared_ptr<ByteBuffer>(buf, deleter);
        }
        catch (const std::bad_alloc& e)
        {
            throw std::runtime_error("Huge page memory allocation failed: " + std::string(e.what()) +
                ", error code: " + std::string(strerror(errno)));
        }
    }
    else
    {
        // Normal memory allocation path
        int toAllocate = blockEnd(size) + (size == 1 ? 0 : fsBlockSize);
        uint8_t* directBufferPointer;

        // Allocate aligned memory using posix_memalign
        if (posix_memalign((void**)&directBufferPointer, fsBlockSize, toAllocate) != 0)
        {
            throw std::runtime_error("Normal memory allocation failed: " + std::string(strerror(errno)));
        }

        // Create custom deleter to free memory
        auto deleter = [](ByteBuffer* buf)
        {
            if (buf && buf->getBuffer())
            {
                free(const_cast<uint8_t*>(buf->getBuffer()));
            }
            // delete buf;
        };

        // Create ByteBuffer and return shared_ptr
        ByteBuffer* buf = new ByteBuffer(directBufferPointer, toAllocate, false);
        return std::shared_ptr<ByteBuffer>(buf, deleter);
    }
}

int DirectIoLib::getToAllocate(int size)
{
    return blockEnd(size) + (size == 1 ? 0 : fsBlockSize);
}


std::shared_ptr<ByteBuffer> DirectIoLib::read(int fd, long fileOffset,
                                              std::shared_ptr<ByteBuffer> directBuffer, long length)
{
    // the file will be read from blockStart(fileOffset), and the first fileDelta bytes should be ignored.
    long fileOffsetAligned = blockStart(fileOffset);
    long toRead = blockEnd(fileOffset + length) - blockStart(fileOffset);
    ssize_t ret = pread(fd, directBuffer->getPointer(), toRead, fileOffsetAligned);
    if (ret == -1)
    {
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
