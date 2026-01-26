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
#include "physical/DynamicBufferPool.h"
#include "utils/ConfigFactory.h"
#include <cstring>

// Thread-local static member initialization
thread_local struct io_uring* DynamicBufferPool::ring = nullptr;
thread_local struct iovec* DynamicBufferPool::iovecs = nullptr;
thread_local uint32_t DynamicBufferPool::maxBufferSlots = 0;
thread_local uint32_t DynamicBufferPool::currentUsedSlots = 0;
thread_local bool DynamicBufferPool::isInitialized = false;
thread_local std::vector<std::shared_ptr<ByteBuffer>> DynamicBufferPool::bufferSlots;
thread_local std::queue<uint32_t> DynamicBufferPool::freeSlots;
thread_local std::map<uint32_t, uint32_t> DynamicBufferPool::colToSlot;
thread_local std::map<uint32_t, uint32_t> DynamicBufferPool::slotToCol;
thread_local std::shared_ptr<DirectIoLib> DynamicBufferPool::directIoLib = nullptr;

void DynamicBufferPool::Initialize(struct io_uring* uringRing, uint32_t maxSlots) {
    if (isInitialized) {
        return;
    }
    
    if (uringRing == nullptr) {
        throw InvalidArgumentException("DynamicBufferPool::Initialize: io_uring ring cannot be null");
    }
    
    ring = uringRing;
    maxBufferSlots = maxSlots;
    currentUsedSlots = 0;
    
    // Initialize DirectIoLib for aligned buffer allocation
    int fsBlockSize = 4096; // Default value
    try {
        fsBlockSize = std::stoi(ConfigFactory::Instance().getProperty("localfs.block.size"));
    } catch (...) {
        // Use default if config not available
    }
    directIoLib = std::make_shared<DirectIoLib>(fsBlockSize);
    
    // Allocate iovec array for all slots
    iovecs = (struct iovec*)calloc(maxSlots, sizeof(struct iovec));
    if (iovecs == nullptr) {
        throw InvalidArgumentException("DynamicBufferPool::Initialize: failed to allocate iovecs");
    }
    
    // Initialize all iovecs to null (sparse registration)
    for (uint32_t i = 0; i < maxSlots; i++) {
        iovecs[i].iov_base = nullptr;
        iovecs[i].iov_len = 0;
    }
    
    // Initialize bufferSlots vector and freeSlots queue
    bufferSlots.resize(maxSlots, nullptr);
    for (uint32_t i = 0; i < maxSlots; i++) {
        freeSlots.push(i);
    }
    
    // Register buffers using sparse registration
    // Note: io_uring_register_buffers with all null entries creates sparse registration
    int ret = io_uring_register_buffers(ring, iovecs, maxSlots);
    if (ret != 0) {
        free(iovecs);
        iovecs = nullptr;
        throw InvalidArgumentException(
            "DynamicBufferPool::Initialize: failed to register buffers (sparse), error: " + 
            std::to_string(ret)
        );
    }
    
    isInitialized = true;
}

std::shared_ptr<ByteBuffer> DynamicBufferPool::AllocateBuffer(uint32_t colId, uint64_t size) {
    if (!isInitialized) {
        throw InvalidArgumentException("DynamicBufferPool::AllocateBuffer: pool not initialized");
    }
    
    // Check if buffer already exists for this colId
    if (colToSlot.find(colId) != colToSlot.end()) {
        throw InvalidArgumentException(
            "DynamicBufferPool::AllocateBuffer: buffer already exists for colId " + 
            std::to_string(colId)
        );
    }
    
    // Allocate a free slot
    int slotIndex = AllocateSlot();
    if (slotIndex < 0) {
        throw InvalidArgumentException("DynamicBufferPool::AllocateBuffer: no free slots available");
    }
    
    // Allocate aligned buffer
    auto buffer = directIoLib->allocateDirectBuffer(size);
    if (buffer == nullptr) {
        FreeSlot(slotIndex);
        throw InvalidArgumentException("DynamicBufferPool::AllocateBuffer: failed to allocate buffer");
    }
    
    // Zero out the buffer
    memset(buffer->getPointer(), 0, buffer->size());
    
    // Store buffer in slot
    bufferSlots[slotIndex] = buffer;
    
    // Update mappings
    colToSlot[colId] = slotIndex;
    slotToCol[slotIndex] = colId;
    
    // Update io_uring buffer registration
    if (!UpdateBufferRegistration(slotIndex, buffer)) {
        // Rollback on failure
        bufferSlots[slotIndex] = nullptr;
        colToSlot.erase(colId);
        slotToCol.erase(slotIndex);
        FreeSlot(slotIndex);
        throw InvalidArgumentException("DynamicBufferPool::AllocateBuffer: failed to update buffer registration");
    }
    
    currentUsedSlots++;
    
    return buffer;
}

std::shared_ptr<ByteBuffer> DynamicBufferPool::GetBuffer(uint32_t colId) {
    auto it = colToSlot.find(colId);
    if (it == colToSlot.end()) {
        return nullptr;
    }
    
    uint32_t slotIndex = it->second;
    return bufferSlots[slotIndex];
}

int DynamicBufferPool::GetBufferSlotIndex(uint32_t colId) {
    auto it = colToSlot.find(colId);
    if (it == colToSlot.end()) {
        return -1;
    }
    return static_cast<int>(it->second);
}

std::shared_ptr<ByteBuffer> DynamicBufferPool::GrowBuffer(uint32_t colId, uint64_t newSize) {
    if (!isInitialized) {
        throw InvalidArgumentException("DynamicBufferPool::GrowBuffer: pool not initialized");
    }
    
    auto it = colToSlot.find(colId);
    if (it == colToSlot.end()) {
        throw InvalidArgumentException("DynamicBufferPool::GrowBuffer: buffer not found for colId " + std::to_string(colId));
    }
    
    uint32_t slotIndex = it->second;
    auto oldBuffer = bufferSlots[slotIndex];
    
    if (oldBuffer == nullptr) {
        throw InvalidArgumentException("DynamicBufferPool::GrowBuffer: buffer slot is null");
    }
    
    if (newSize <= oldBuffer->size()) {
        // No need to grow
        return oldBuffer;
    }
    
    // Allocate new larger buffer
    auto newBuffer = directIoLib->allocateDirectBuffer(newSize);
    if (newBuffer == nullptr) {
        throw InvalidArgumentException("DynamicBufferPool::GrowBuffer: failed to allocate new buffer");
    }
    
    // Copy old data to new buffer
    memcpy(newBuffer->getPointer(), oldBuffer->getPointer(), oldBuffer->size());
    // Zero out the extended part
    memset(static_cast<uint8_t*>(newBuffer->getPointer()) + oldBuffer->size(), 0, newSize - oldBuffer->size());
    
    // Update buffer slot
    bufferSlots[slotIndex] = newBuffer;
    
    // Update io_uring registration
    if (!UpdateBufferRegistration(slotIndex, newBuffer)) {
        // Rollback on failure
        bufferSlots[slotIndex] = oldBuffer;
        throw InvalidArgumentException("DynamicBufferPool::GrowBuffer: failed to update buffer registration");
    }
    
    return newBuffer;
}

void DynamicBufferPool::ReleaseBuffer(uint32_t colId) {
    auto it = colToSlot.find(colId);
    if (it == colToSlot.end()) {
        return; // Already released or never allocated
    }
    
    uint32_t slotIndex = it->second;
    
    // Clear buffer slot
    bufferSlots[slotIndex] = nullptr;
    
    // Update iovec to null (unregister from io_uring)
    iovecs[slotIndex].iov_base = nullptr;
    iovecs[slotIndex].iov_len = 0;
    
    // Update io_uring registration (set to null)
    struct iovec nullIov;
    nullIov.iov_base = nullptr;
    nullIov.iov_len = 0;
    io_uring_register_buffers_update_tag(ring, slotIndex, &nullIov, NULL, 1);
    
    // Remove mappings
    slotToCol.erase(slotIndex);
    colToSlot.erase(colId);
    
    // Return slot to free pool
    FreeSlot(slotIndex);
    
    if (currentUsedSlots > 0) {
        currentUsedSlots--;
    }
}

bool DynamicBufferPool::IsInitialized() {
    return isInitialized;
}

uint32_t DynamicBufferPool::GetBufferCount() {
    return currentUsedSlots;
}

uint32_t DynamicBufferPool::GetMaxSlots() {
    return maxBufferSlots;
}

void DynamicBufferPool::Reset() {
    if (!isInitialized) {
        return;
    }
    
    // Unregister buffers from io_uring
    if (ring != nullptr) {
        io_uring_unregister_buffers(ring);
    }
    
    // Clear all data structures
    bufferSlots.clear();
    while (!freeSlots.empty()) {
        freeSlots.pop();
    }
    colToSlot.clear();
    slotToCol.clear();
    
    // Free iovecs
    if (iovecs != nullptr) {
        free(iovecs);
        iovecs = nullptr;
    }
    
    // Reset state
    ring = nullptr;
    maxBufferSlots = 0;
    currentUsedSlots = 0;
    isInitialized = false;
    directIoLib = nullptr;
}

std::shared_ptr<DirectIoLib> DynamicBufferPool::GetDirectIoLib() {
    return directIoLib;
}

// Private methods

int DynamicBufferPool::AllocateSlot() {
    if (freeSlots.empty()) {
        return -1; // No free slots available
    }
    
    uint32_t slotIndex = freeSlots.front();
    freeSlots.pop();
    return static_cast<int>(slotIndex);
}

void DynamicBufferPool::FreeSlot(uint32_t slotIndex) {
    if (slotIndex >= maxBufferSlots) {
        return;
    }
    freeSlots.push(slotIndex);
}

bool DynamicBufferPool::UpdateBufferRegistration(uint32_t slotIndex, std::shared_ptr<ByteBuffer> buffer) {
    if (slotIndex >= maxBufferSlots || buffer == nullptr) {
        return false;
    }
    
    // Update iovec
    iovecs[slotIndex].iov_base = buffer->getPointer();
    iovecs[slotIndex].iov_len = buffer->size();
    
    // Use io_uring_register_buffers_update_tag to dynamically update the buffer
    // This updates a single buffer at the specified offset
    int ret = io_uring_register_buffers_update_tag(ring, slotIndex, &iovecs[slotIndex], NULL, 1);
    
    if (ret < 0) {
        // Rollback iovec on failure
        iovecs[slotIndex].iov_base = nullptr;
        iovecs[slotIndex].iov_len = 0;
        return false;
    }
    
    return true;
}
