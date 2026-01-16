/*
 * Copyright 2025 PixelsDB.
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

#include "EpochManager.h"

// Thread-local storage for thread info
thread_local EpochManager::ThreadInfo* EpochManager::tlsThreadInfo = nullptr;

EpochManager::~EpochManager() {
    ThreadInfo* current = threadListHead.load(std::memory_order_acquire);
    while (current) {
        ThreadInfo* next = current->next;
        delete current;
        current = next;
    }
}

EpochManager::ThreadInfo* EpochManager::getOrCreateThreadInfo() {
    if (tlsThreadInfo) {
        return tlsThreadInfo;
    }

    // Create new thread info
    ThreadInfo* newInfo = new ThreadInfo();
    newInfo->threadId = std::this_thread::get_id();

    // Add to global list
    std::lock_guard<std::mutex> lock(threadListMutex);
    newInfo->next = threadListHead.load(std::memory_order_relaxed);
    threadListHead.store(newInfo, std::memory_order_release);

    tlsThreadInfo = newInfo;
    return newInfo;
}

void EpochManager::enterEpoch() {
    ThreadInfo* info = getOrCreateThreadInfo();
    uint64_t currentEpoch = globalEpoch.load(std::memory_order_acquire);
    info->localEpoch.store(currentEpoch, std::memory_order_release);
}

void EpochManager::exitEpoch() {
    if (tlsThreadInfo) {
        tlsThreadInfo->localEpoch.store(0, std::memory_order_release);
    }
}

uint64_t EpochManager::advanceEpoch() {
    return globalEpoch.fetch_add(1, std::memory_order_acq_rel) + 1;
}

bool EpochManager::canReclaim(uint64_t retireEpoch) const {
    // Scan all threads to find the minimum active epoch
    ThreadInfo* current = threadListHead.load(std::memory_order_acquire);
    
    while (current) {
        if (!current->active.load(std::memory_order_acquire)) {
            current = current->next;
            continue;
        }

        uint64_t localEpoch = current->localEpoch.load(std::memory_order_acquire);
        
        // localEpoch == 0 means the thread is not in critical section, skip it
        if (localEpoch != 0 && localEpoch <= retireEpoch) {
            // Found a thread still in or before the retire epoch
            return false;
        }
        
        current = current->next;
    }
    
    // All threads have advanced past the retire epoch
    return true;
}
