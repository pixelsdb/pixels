/*
 * Copyright 2026 PixelsDB.
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
 * @author whz
 * @create 2026-01-23
 */
#include "physical/natives/DirectUringRandomAccessFileDynamic.h"

// Thread-local static member initialization
thread_local struct io_uring* DirectUringRandomAccessFileDynamic::ring = nullptr;
thread_local bool DirectUringRandomAccessFileDynamic::isInitialized = false;
thread_local uint32_t DirectUringRandomAccessFileDynamic::queueDepth = 0;

DirectUringRandomAccessFileDynamic::DirectUringRandomAccessFileDynamic(const std::string& file) 
    : DirectRandomAccessFile(file) {
    // File is opened by base class DirectRandomAccessFile
}

void DirectUringRandomAccessFileDynamic::Initialize(uint32_t qd, uint32_t maxBufferSlots) {
    if (isInitialized) {
        return;
    }
    
    queueDepth = qd;
    
    // Initialize io_uring ring
    ring = new io_uring();
    if (io_uring_queue_init(queueDepth, ring, 0) < 0) {
        delete ring;
        ring = nullptr;
        throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::Initialize: failed to initialize io_uring");
    }
    
    // Initialize DynamicBufferPool with the ring
    try {
        DynamicBufferPool::Initialize(ring, maxBufferSlots);
    } catch (const std::exception& e) {
        io_uring_queue_exit(ring);
        delete ring;
        ring = nullptr;
        throw InvalidArgumentException(
            std::string("DirectUringRandomAccessFileDynamic::Initialize: failed to initialize buffer pool: ") + e.what()
        );
    }
    
    isInitialized = true;
}

void DirectUringRandomAccessFileDynamic::Reset() {
    if (!isInitialized) {
        return;
    }
    
    // Reset buffer pool first
    DynamicBufferPool::Reset();
    
    // Cleanup io_uring
    if (ring != nullptr) {
        io_uring_queue_exit(ring);
        delete ring;
        ring = nullptr;
    }
    
    isInitialized = false;
    queueDepth = 0;
}

DynamicBufferPool* DirectUringRandomAccessFileDynamic::GetBufferPool() {
    // Return the buffer pool instance (static class, so we return nullptr as placeholder)
    // Users should call DynamicBufferPool methods directly
    return nullptr;
}

std::shared_ptr<ByteBuffer> DirectUringRandomAccessFileDynamic::readAsync(int length, uint32_t colId) {
    if (!isInitialized) {
        throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::readAsync: not initialized");
    }
    
    // Get buffer from pool
    auto buffer = DynamicBufferPool::GetBuffer(colId);
    if (buffer == nullptr) {
        throw InvalidArgumentException(
            "DirectUringRandomAccessFileDynamic::readAsync: buffer not found for colId " + 
            std::to_string(colId)
        );
    }
    
    // Get slot index for this column
    int slotIndex = DynamicBufferPool::GetBufferSlotIndex(colId);
    if (slotIndex < 0) {
        throw InvalidArgumentException(
            "DirectUringRandomAccessFileDynamic::readAsync: invalid slot index for colId " + 
            std::to_string(colId)
        );
    }
    
    return readAsync(length, buffer, slotIndex);
}

std::shared_ptr<ByteBuffer> DirectUringRandomAccessFileDynamic::readAsync(
    int length, 
    std::shared_ptr<ByteBuffer> buffer, 
    int slotIndex) {
    
    if (!isInitialized) {
        throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::readAsync: not initialized");
    }
    
    if (buffer == nullptr) {
        throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::readAsync: buffer is null");
    }
    
    // Validate slot index
    if (slotIndex < 0 || slotIndex >= DynamicBufferPool::GetMaxSlots()) {
        throw InvalidArgumentException(
            "DirectUringRandomAccessFileDynamic::readAsync: invalid slot index " + 
            std::to_string(slotIndex)
        );
    }
    
    // Get submission queue entry
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::readAsync: failed to get sqe");
    }
    
    if (enableDirect) {
        // Direct I/O: need to align offsets and sizes
        auto directIoLib = DynamicBufferPool::GetDirectIoLib();
        if (directIoLib == nullptr) {
            throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::readAsync: DirectIoLib not initialized");
        }
        
        uint64_t fileOffsetAligned = directIoLib->blockStart(offset);
        uint64_t toRead = directIoLib->blockEnd(offset + length) - fileOffsetAligned;
        
        // Check if buffer is large enough
        if (toRead > buffer->size()) {
            throw InvalidArgumentException(
                "DirectUringRandomAccessFileDynamic::readAsync: buffer size " + 
                std::to_string(buffer->size()) + " is too small for required " + 
                std::to_string(toRead) + " bytes (caller should resize buffer before calling)"
            );
        }
        
        // Prepare fixed buffer read operation
        io_uring_prep_read_fixed(sqe, fd, buffer->getPointer(), toRead, fileOffsetAligned, slotIndex);
        
        // Create view of the actual data (excluding alignment padding)
        auto result = std::make_shared<ByteBuffer>(*buffer, offset - fileOffsetAligned, length);
        seek(offset + length);
        return result;
    } else {
        // Regular I/O
        if (length > buffer->size()) {
            throw InvalidArgumentException(
                "DirectUringRandomAccessFileDynamic::readAsync: buffer size " + 
                std::to_string(buffer->size()) + " is too small for required " + 
                std::to_string(length) + " bytes (caller should resize buffer before calling)"
            );
        }
        
        // Prepare fixed buffer read operation
        io_uring_prep_read_fixed(sqe, fd, buffer->getPointer(), length, offset, slotIndex);
        
        seek(offset + length);
        return std::make_shared<ByteBuffer>(*buffer, 0, length);
    }
}

void DirectUringRandomAccessFileDynamic::readAsyncSubmit(int count) {
    if (!isInitialized) {
        throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::readAsyncSubmit: not initialized");
    }
    
    int ret = io_uring_submit(ring);
    if (ret != count) {
        throw InvalidArgumentException(
            "DirectUringRandomAccessFileDynamic::readAsyncSubmit: expected " + 
            std::to_string(count) + " submissions, got " + std::to_string(ret)
        );
    }
}

void DirectUringRandomAccessFileDynamic::readAsyncComplete(int count) {
    if (!isInitialized) {
        throw InvalidArgumentException("DirectUringRandomAccessFileDynamic::readAsyncComplete: not initialized");
    }
    
    struct io_uring_cqe* cqe;
    for (int i = 0; i < count; i++) {
        if (io_uring_wait_cqe_nr(ring, &cqe, 1) != 0) {
            throw InvalidArgumentException(
                "DirectUringRandomAccessFileDynamic::readAsyncComplete: failed to wait for completion"
            );
        }
        
        // Check for errors in completion
        if (cqe->res < 0) {
            int err = -cqe->res;
            io_uring_cqe_seen(ring, cqe);
            throw InvalidArgumentException(
                "DirectUringRandomAccessFileDynamic::readAsyncComplete: I/O error " + std::to_string(err)
            );
        }
        
        io_uring_cqe_seen(ring, cqe);
    }
}

void DirectUringRandomAccessFileDynamic::readAsyncExecute(int count) {
    readAsyncSubmit(count);
    readAsyncComplete(count);
}

bool DirectUringRandomAccessFileDynamic::IsInitialized() {
    return isInitialized;
}

DirectUringRandomAccessFileDynamic::~DirectUringRandomAccessFileDynamic() {
    // File descriptor is closed by base class
    // io_uring and buffer pool are managed by static methods
}
