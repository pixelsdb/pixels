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
#ifndef PIXELS_RETINA_EPOCH_MANAGER_H
#define PIXELS_RETINA_EPOCH_MANAGER_H

#include <atomic>
#include <thread>
#include <mutex>

/**
 * EpochManager - A global epoch-based memory reclamation system
 * 
 * This manager allows safe deferred deletion of objects in a concurrent
 * environment using epoch-based reclamation. Threads announce their presence
 * in an epoch, and objects can only be reclaimed when all threads have
 * advanced past the epoch in which the object was retired.
 */
class EpochManager {
public:
    static EpochManager& getInstance() {
        static EpochManager instance;
        return instance;
    }

    /**
     * Enter the critical section. Thread announces it's participating in
     * the current epoch.
     */
    void enterEpoch();

    /**
     * Exit the critical section. Thread leaves the epoch.
     */
    void exitEpoch();

    /**
     * Advance the global epoch counter. Should be called by GC operations.
     * Returns the new epoch value.
     */
    uint64_t advanceEpoch();

    /**
     * Check if an object retired at the given epoch can be safely reclaimed.
     * An object can be reclaimed if all threads have advanced past its retire epoch.
     */
    bool canReclaim(uint64_t retireEpoch) const;

    /**
     * Get the current global epoch.
     */
    uint64_t getCurrentEpoch() const {
        return globalEpoch.load(std::memory_order_acquire);
    }

private:
    EpochManager() : globalEpoch(1) {}
    ~EpochManager();

    EpochManager(const EpochManager&) = delete;
    EpochManager& operator=(const EpochManager&) = delete;

    struct ThreadInfo {
        std::atomic<uint64_t> localEpoch{0};  // 0 means not in critical section
        std::atomic<bool> active{true};
        std::thread::id threadId;
        ThreadInfo* next{nullptr};
    };

    std::atomic<uint64_t> globalEpoch;
    std::atomic<ThreadInfo*> threadListHead{nullptr};
    std::mutex threadListMutex;  // Protects thread list modifications

    ThreadInfo* getOrCreateThreadInfo();
    static thread_local ThreadInfo* tlsThreadInfo;
};

/**
 * RAII helper for epoch protection
 */
class EpochGuard {
public:
    EpochGuard() {
        EpochManager::getInstance().enterEpoch();
    }

    ~EpochGuard() {
        EpochManager::getInstance().exitEpoch();
    }

    EpochGuard(const EpochGuard&) = delete;
    EpochGuard& operator=(const EpochGuard&) = delete;
};

#endif // PIXELS_RETINA_EPOCH_MANAGER_H
