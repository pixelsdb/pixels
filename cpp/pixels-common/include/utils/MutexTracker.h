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

/*
 * @author whz
 * @create 2025-08-23
 */

#ifndef DUCKDB_MUTEX_H
#define DUCKDB_MUTEX_H
#include <mutex>
#include <thread>
#include <unordered_map>
#include <stdexcept>
#include <iostream>
#include <string>
class MutexTracker {
private:
    // Stores the mapping between mutexes and their owning thread IDs
    std::unordered_map<const std::mutex*, std::thread::id> mutex_owners;
    // Internal mutex to protect the mapping table
    std::mutex internal_mutex;

public:
    // Gets the thread ID of the mutex owner; returns default ID if not held
    std::thread::id get_owner(const std::mutex* mutex) {
        std::lock_guard<std::mutex> lock(internal_mutex);
        auto it = mutex_owners.find(mutex);
        if (it != mutex_owners.end()) {
            return it->second;
        }
        return std::thread::id(); // Not held by any thread
    }

    // Records that a mutex has been acquired
    void lock_acquired(const std::mutex* mutex) {
        std::lock_guard<std::mutex> lock(internal_mutex);
        auto current_thread = std::this_thread::get_id();

        // Check for duplicate acquisition (to prevent recursive lock issues)
        auto it = mutex_owners.find(mutex);
        if (it != mutex_owners.end()) {
            if (it->second == current_thread) {
                // Same thread acquiring a non-recursive mutex repeatedly, may cause deadlock
                throw std::runtime_error("Same thread acquiring non-recursive mutex repeatedly");
            }
        }

        mutex_owners[mutex] = current_thread;
    }

    // Records that a mutex has been released
    void lock_released(const std::mutex* mutex) {
        std::lock_guard<std::mutex> lock(internal_mutex);
        mutex_owners.erase(mutex);
    }

    // Prints information about the mutex owner
    void print_owner(const std::mutex* mutex, const std::string& mutex_name) {
        std::thread::id owner = get_owner(mutex);
        if (owner == std::thread::id()) {
            std::cout << mutex_name << " is not held by any thread" << std::endl;
        } else {
            std::cout << mutex_name << " is held by thread " << std::hash<std::thread::id>{}(owner)
                      << std::endl;
        }
    }
};


// Mutex wrapper with tracking functionality
class TrackedMutex {
private:
    std::mutex internal_mutex;
    std::string name;
    MutexTracker g_mutex_tracker;

public:
    TrackedMutex(const std::string& name, MutexTracker& g_mutex_tracker) : name(name) {}

    // Locks the mutex and records the owner
    void lock() {
        internal_mutex.lock();
        g_mutex_tracker.lock_acquired(&internal_mutex);
    }

    // Unlocks the mutex and clears the owner record
    void unlock() {
        g_mutex_tracker.lock_released(&internal_mutex);
        internal_mutex.unlock();
    }

    // Tries to lock the mutex (records owner if successful)
    bool try_lock() {
        if (internal_mutex.try_lock()) {
            g_mutex_tracker.lock_acquired(&internal_mutex);
            return true;
        }
        return false;
    }

    // Gets the mutex name
    const std::string& get_name() const { return name; }

    // Gets pointer to internal mutex (for tracking purposes)
    const std::mutex* get_internal_mutex() const {
        return &internal_mutex;
    }
};

// RAII lock manager for use with TrackedMutex
template<typename Mutex>
class TrackedLockGuard {
private:
    Mutex& mutex;

public:
    TrackedLockGuard(Mutex& m) : mutex(m) {
        mutex.lock();
    }

    ~TrackedLockGuard() {
        mutex.unlock();
    }

    // Disable copying
    TrackedLockGuard(const TrackedLockGuard&) = delete;
    TrackedLockGuard& operator=(const TrackedLockGuard&) = delete;
};


#endif // DUCKDB_MUTEX_H
