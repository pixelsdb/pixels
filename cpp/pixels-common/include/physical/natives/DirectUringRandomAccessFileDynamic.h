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
#ifndef PIXELS_DIRECTURINGRANDOMACCESSFILEDYNAMIC_H
#define PIXELS_DIRECTURINGRANDOMACCESSFILEDYNAMIC_H

#include "liburing.h"
#include "liburing/io_uring.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/DynamicBufferPool.h"
#include "exception/InvalidArgumentException.h"
#include "DirectIoLib.h"
#include <vector>
#include <memory>

/**
 * DirectUringRandomAccessFileDynamic provides asynchronous file I/O using io_uring
 * with dynamic buffer management through DynamicBufferPool.
 * 
 * This version supports:
 * - Dynamic buffer allocation and growth
 * - Sparse buffer registration (IORING_RSRC_REGISTER_SPARSE)
 * - Runtime buffer updates (IORING_REGISTER_BUFFERS_UPDATE)
 * - Batch read operations
 */
class DirectUringRandomAccessFileDynamic : public DirectRandomAccessFile {
public:
    /**
     * Construct a DirectUringRandomAccessFileDynamic
     * @param file File path to open
     */
    explicit DirectUringRandomAccessFileDynamic(const std::string& file);
    
    /**
     * Initialize io_uring and DynamicBufferPool
     * @param queueDepth io_uring queue depth (default: 4096)
     * @param maxBufferSlots Maximum buffer slots for sparse registration (default: 1024)
     */
    static void Initialize(uint32_t queueDepth = 4096, uint32_t maxBufferSlots = DEFAULT_MAX_BUFFER_SLOTS);
    
    /**
     * Reset and cleanup io_uring and buffer pool
     */
    static void Reset();
    
    /**
     * Get the DynamicBufferPool instance
     */
    static DynamicBufferPool* GetBufferPool();
    
    /**
     * Read data asynchronously using registered buffer
     * @param length Number of bytes to read
     * @param colId Column ID to identify the buffer
     * @return ByteBuffer view containing the read data
     */
    std::shared_ptr<ByteBuffer> readAsync(int length, uint32_t colId);
    
    /**
     * Read data asynchronously using registered buffer with explicit buffer
     * @param length Number of bytes to read
     * @param buffer Buffer to read into (must be registered in DynamicBufferPool)
     * @param slotIndex Buffer slot index for io_uring fixed buffer read
     * @return ByteBuffer view containing the read data
     */
    std::shared_ptr<ByteBuffer> readAsync(int length, std::shared_ptr<ByteBuffer> buffer, int slotIndex);
    
    /**
     * Submit pending async read operations
     * @param count Number of operations to submit
     */
    void readAsyncSubmit(int count);
    
    /**
     * Wait for async read operations to complete
     * @param count Number of operations to wait for
     */
    void readAsyncComplete(int count);
    
    /**
     * Convenience method: submit and complete in one call
     * @param count Number of operations
     */
    void readAsyncExecute(int count);
    
    /**
     * Check if the file reader is initialized
     */
    static bool IsInitialized();
    
    /**
     * Destructor
     */
    ~DirectUringRandomAccessFileDynamic();

private:
    // Thread-local io_uring instance
    static thread_local struct io_uring* ring;
    static thread_local bool isInitialized;
    static thread_local uint32_t queueDepth;
};

#endif // PIXELS_DIRECTURINGRANDOMACCESSFILEDYNAMIC_H
