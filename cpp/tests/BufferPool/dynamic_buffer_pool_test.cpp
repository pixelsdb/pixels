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

#include <iostream>
#include <fstream>
#include <cstring>
#include <cassert>
#include "physical/DynamicBufferPool.h"
#include "physical/natives/DirectUringRandomAccessFileDynamic.h"
#include "utils/ConfigFactory.h"

// Test file content size
#define TEST_FILE_SIZE (1024 * 1024) // 1MB
#define TEST_BUFFER_SIZE (64 * 1024)  // 64KB

/**
 * Create a test file with known content
 */
void createTestFile(const std::string& filename, size_t size) {
    std::ofstream file(filename, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Failed to create test file: " + filename);
    }
    
    // Write pattern: repeating sequence 0, 1, 2, ..., 255
    for (size_t i = 0; i < size; i++) {
        uint8_t value = static_cast<uint8_t>(i % 256);
        file.write(reinterpret_cast<const char*>(&value), 1);
    }
    
    file.close();
    std::cout << "Created test file: " << filename << " (" << size << " bytes)" << std::endl;
}

/**
 * Test 1: DynamicBufferPool basic operations
 */
void testDynamicBufferPoolBasics() {
    std::cout << "\n=== Test 1: DynamicBufferPool Basic Operations ===" << std::endl;
    
    // Initialize io_uring
    struct io_uring ring;
    if (io_uring_queue_init(256, &ring, 0) < 0) {
        throw std::runtime_error("Failed to initialize io_uring");
    }
    
    try {
        // Initialize buffer pool
        DynamicBufferPool::Initialize(&ring, 128);
        assert(DynamicBufferPool::IsInitialized());
        std::cout << "✓ Buffer pool initialized" << std::endl;
        
        // Allocate some buffers
        auto buf1 = DynamicBufferPool::AllocateBuffer(0, TEST_BUFFER_SIZE);
        assert(buf1 != nullptr);
        assert(buf1->size() >= TEST_BUFFER_SIZE);
        std::cout << "✓ Allocated buffer for colId=0, size=" << buf1->size() << std::endl;
        
        auto buf2 = DynamicBufferPool::AllocateBuffer(1, TEST_BUFFER_SIZE * 2);
        assert(buf2 != nullptr);
        assert(buf2->size() >= TEST_BUFFER_SIZE * 2);
        std::cout << "✓ Allocated buffer for colId=1, size=" << buf2->size() << std::endl;
        
        // Check buffer count
        assert(DynamicBufferPool::GetBufferCount() == 2);
        std::cout << "✓ Buffer count: " << DynamicBufferPool::GetBufferCount() << std::endl;
        
        // Get buffer by colId
        auto retrieved = DynamicBufferPool::GetBuffer(0);
        assert(retrieved == buf1);
        std::cout << "✓ Retrieved buffer for colId=0" << std::endl;
        
        // Get slot index
        int slotIdx = DynamicBufferPool::GetBufferSlotIndex(0);
        assert(slotIdx >= 0);
        std::cout << "✓ Slot index for colId=0: " << slotIdx << std::endl;
        
        // Test buffer growth
        size_t originalSize = buf1->size();
        auto buf1_grown = DynamicBufferPool::GrowBuffer(0, TEST_BUFFER_SIZE * 3);
        assert(buf1_grown != nullptr);
        assert(buf1_grown->size() >= TEST_BUFFER_SIZE * 3);
        assert(buf1_grown->size() > originalSize);
        std::cout << "✓ Grew buffer for colId=0 from " << originalSize 
                  << " to " << buf1_grown->size() << " bytes" << std::endl;
        
        // Release a buffer
        DynamicBufferPool::ReleaseBuffer(1);
        assert(DynamicBufferPool::GetBufferCount() == 1);
        assert(DynamicBufferPool::GetBuffer(1) == nullptr);
        std::cout << "✓ Released buffer for colId=1" << std::endl;
        
        // Cleanup
        DynamicBufferPool::Reset();
        assert(!DynamicBufferPool::IsInitialized());
        std::cout << "✓ Buffer pool reset" << std::endl;
        
    } catch (const std::exception& e) {
        io_uring_queue_exit(&ring);
        throw;
    }
    
    io_uring_queue_exit(&ring);
    std::cout << "✓ Test 1 passed!" << std::endl;
}

/**
 * Test 2: DirectUringRandomAccessFileDynamic read operations
 */
void testDirectUringRead() {
    std::cout << "\n=== Test 2: DirectUringRandomAccessFileDynamic Read Operations ===" << std::endl;
    
    const std::string testFile = "/tmp/test_uring_dynamic.dat";
    createTestFile(testFile, TEST_FILE_SIZE);
    
    try {
        // Initialize
        DirectUringRandomAccessFileDynamic::Initialize(256, 128);
        assert(DirectUringRandomAccessFileDynamic::IsInitialized());
        std::cout << "✓ DirectUringRandomAccessFileDynamic initialized" << std::endl;
        
        // Allocate buffers for columns
        DynamicBufferPool::AllocateBuffer(0, TEST_BUFFER_SIZE);
        DynamicBufferPool::AllocateBuffer(1, TEST_BUFFER_SIZE);
        std::cout << "✓ Allocated buffers for 2 columns" << std::endl;
        
        // Open file
        DirectUringRandomAccessFileDynamic reader(testFile);
        std::cout << "✓ Opened test file" << std::endl;
        
        // Read using column 0 buffer
        reader.seek(0);
        auto data1 = reader.readAsync(1024, 0);
        reader.readAsyncExecute(1);
        
        // Verify content
        assert(data1->size() == 1024);
        for (int i = 0; i < 256; i++) {
            uint8_t expected = static_cast<uint8_t>(i % 256);
            uint8_t actual = data1->get(i);
            assert(actual == expected);
        }
        std::cout << "✓ Read and verified 1024 bytes using colId=0" << std::endl;
        
        // Read using column 1 buffer
        reader.seek(4096);
        auto data2 = reader.readAsync(2048, 1);
        reader.readAsyncExecute(1);
        
        assert(data2->size() == 2048);
        for (int i = 0; i < 256; i++) {
            uint8_t expected = static_cast<uint8_t>((4096 + i) % 256);
            uint8_t actual = data2->get(i);
            assert(actual == expected);
        }
        std::cout << "✓ Read and verified 2048 bytes using colId=1" << std::endl;
        
        // Test batch reads
        reader.seek(8192);
        auto data3 = reader.readAsync(512, 0);
        reader.seek(16384);
        auto data4 = reader.readAsync(512, 1);
        reader.readAsyncExecute(2);
        
        assert(data3->size() == 512);
        assert(data4->size() == 512);
        std::cout << "✓ Batch read completed (2 operations)" << std::endl;
        
        // Cleanup
        DirectUringRandomAccessFileDynamic::Reset();
        std::cout << "✓ DirectUringRandomAccessFileDynamic reset" << std::endl;
        
        // Remove test file
        std::remove(testFile.c_str());
        
    } catch (const std::exception& e) {
        std::remove(testFile.c_str());
        throw;
    }
    
    std::cout << "✓ Test 2 passed!" << std::endl;
}

/**
 * Test 3: Dynamic buffer growth during I/O
 */
void testDynamicGrowth() {
    std::cout << "\n=== Test 3: Dynamic Buffer Growth During I/O ===" << std::endl;
    
    const std::string testFile = "/tmp/test_uring_growth.dat";
    createTestFile(testFile, TEST_FILE_SIZE);
    
    try {
        // Initialize with small initial buffer
        DirectUringRandomAccessFileDynamic::Initialize(256, 128);
        
        // Allocate small buffer
        size_t smallSize = 4096;
        DynamicBufferPool::AllocateBuffer(0, smallSize);
        std::cout << "✓ Allocated small buffer: " << smallSize << " bytes" << std::endl;
        
        DirectUringRandomAccessFileDynamic reader(testFile);
        
        // Try to read more than buffer size - should auto-grow
        reader.seek(0);
        size_t largeReadSize = 16384; // 16KB > 4KB initial
        auto data = reader.readAsync(largeReadSize, 0);
        reader.readAsyncExecute(1);
        
        assert(data->size() == largeReadSize);
        auto buf = DynamicBufferPool::GetBuffer(0);
        assert(buf->size() >= largeReadSize);
        std::cout << "✓ Buffer auto-grew from " << smallSize 
                  << " to " << buf->size() << " bytes during read" << std::endl;
        
        // Verify content
        for (int i = 0; i < 256; i++) {
            uint8_t expected = static_cast<uint8_t>(i % 256);
            uint8_t actual = data->get(i);
            assert(actual == expected);
        }
        std::cout << "✓ Data verified after auto-growth" << std::endl;
        
        // Cleanup
        DirectUringRandomAccessFileDynamic::Reset();
        std::remove(testFile.c_str());
        
    } catch (const std::exception& e) {
        std::remove(testFile.c_str());
        throw;
    }
    
    std::cout << "✓ Test 3 passed!" << std::endl;
}

/**
 * Test 4: Sparse buffer allocation stress test
 */
void testSparseAllocation() {
    std::cout << "\n=== Test 4: Sparse Buffer Allocation Stress Test ===" << std::endl;
    
    struct io_uring ring;
    if (io_uring_queue_init(256, &ring, 0) < 0) {
        throw std::runtime_error("Failed to initialize io_uring");
    }
    
    try {
        // Initialize with many slots
        DynamicBufferPool::Initialize(&ring, 512);
        std::cout << "✓ Initialized pool with 512 slots" << std::endl;
        
        // Allocate many buffers (but not all slots)
        const int numBuffers = 100;
        for (uint32_t i = 0; i < numBuffers; i++) {
            auto buf = DynamicBufferPool::AllocateBuffer(i, 4096);
            assert(buf != nullptr);
        }
        std::cout << "✓ Allocated " << numBuffers << " buffers" << std::endl;
        assert(DynamicBufferPool::GetBufferCount() == numBuffers);
        
        // Release some buffers
        for (uint32_t i = 0; i < numBuffers; i += 2) {
            DynamicBufferPool::ReleaseBuffer(i);
        }
        int remainingBuffers = (numBuffers + 1) / 2;
        assert(DynamicBufferPool::GetBufferCount() == remainingBuffers);
        std::cout << "✓ Released half of the buffers, remaining: " << remainingBuffers << std::endl;
        
        // Allocate new buffers in freed slots
        for (uint32_t i = 0; i < numBuffers; i += 2) {
            auto buf = DynamicBufferPool::AllocateBuffer(i + 1000, 8192);
            assert(buf != nullptr);
        }
        assert(DynamicBufferPool::GetBufferCount() == numBuffers);
        std::cout << "✓ Re-allocated buffers in freed slots" << std::endl;
        
        // Cleanup
        DynamicBufferPool::Reset();
        
    } catch (const std::exception& e) {
        io_uring_queue_exit(&ring);
        throw;
    }
    
    io_uring_queue_exit(&ring);
    std::cout << "✓ Test 4 passed!" << std::endl;
}

/**
 * Main test runner
 */
int main(int argc, char** argv) {
    std::cout << "========================================" << std::endl;
    std::cout << "Dynamic Buffer Pool & io_uring Test Suite" << std::endl;
    std::cout << "========================================" << std::endl;
    
    try {
        // Initialize config if needed
        // try {
        //     ConfigFactory::Instance().getProperty("localfs.block.size", "4096");
        // } catch (...) {
        //     // Config might already be initialized
        // }
        
        // Run tests
        testDynamicBufferPoolBasics();
        testDirectUringRead();
        testDynamicGrowth();
        testSparseAllocation();
        
        std::cout << "\n========================================" << std::endl;
        std::cout << "✓ All tests passed successfully!" << std::endl;
        std::cout << "========================================" << std::endl;
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "\n✗ Test failed with exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "\n✗ Test failed with unknown exception" << std::endl;
        return 1;
    }
}
