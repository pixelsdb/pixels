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
 * File Description:
 * This file provides mutex tracking utilities for debugging and diagnosing
 * concurrency issues. It wraps std::mutex to record ownership information
 * (which thread currently holds a mutex) and helps detect incorrect usage
 * such as repeated locking of non-recursive mutexes by the same thread.
 * It used in previous version: global Bufferpool, but it has been deprecated in latest version: threadLocal Bufferpool
 *
 * The main components include:
 *  - MutexTracker: A global tracker that maintains mutex-to-thread ownership.
 *  - TrackedMutex: A mutex wrapper that reports lock/unlock events.
 *  - TrackedLockGuard: An RAII-style lock guard for TrackedMutex.
 *
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

class MutexTracker
{
private:
    // Mapping between mutex pointers and their owning thread IDs
    std::unordered_map<const std::mutex*, std::thread::id> mutexOwners;

    // Internal mutex to protect the ownership map
    std::mutex internalMutex;

public:
    // Returns the owner thread ID of a mutex, or default thread::id if not held
    std::thread::id getOwner(const std::mutex* mutex)
    {
        std::lock_guard<std::mutex> lock(internalMutex);
        auto it = mutexOwners.find(mutex);
        if (it != mutexOwners.end())
        {
            return it->second;
        }
        return std::thread::id();
    }

    // Records that the mutex has been acquired by the current thread
    void lockAcquired(const std::mutex* mutex)
    {
        std::lock_guard<std::mutex> lock(internalMutex);
        auto currentThread = std::this_thread::get_id();

        auto it = mutexOwners.find(mutex);
        if (it != mutexOwners.end() && it->second == currentThread)
        {
            throw std::runtime_error(
                "Same thread acquiring non-recursive mutex repeatedly");
        }

        mutexOwners[mutex] = currentThread;
    }

    // Records that the mutex has been released
    void lockReleased(const std::mutex* mutex)
    {
        std::lock_guard<std::mutex> lock(internalMutex);
        mutexOwners.erase(mutex);
    }

    // Prints the owner information of a mutex
    void printOwner(const std::mutex* mutex, const std::string& mutexName)
    {
        std::thread::id owner = getOwner(mutex);
        if (owner == std::thread::id())
        {
            std::cout << mutexName << " is not held by any thread" << std::endl;
        }
        else
        {
            std::cout << mutexName << " is held by thread "
                      << std::hash<std::thread::id>{}(owner)
                      << std::endl;
        }
    }
};

// Mutex wrapper with ownership tracking
class TrackedMutex
{
private:
    std::mutex internalMutex;
    std::string name;
    MutexTracker& mutexTracker;

public:
    TrackedMutex(const std::string& name, MutexTracker& mutexTracker)
        : name(name), mutexTracker(mutexTracker)
    {
    }

    void lock()
    {
        internalMutex.lock();
        mutexTracker.lockAcquired(&internalMutex);
    }

    void unlock()
    {
        mutexTracker.lockReleased(&internalMutex);
        internalMutex.unlock();
    }

    bool tryLock()
    {
        if (internalMutex.try_lock())
        {
            mutexTracker.lockAcquired(&internalMutex);
            return true;
        }
        return false;
    }

    const std::string& getName() const
    {
        return name;
    }

    const std::mutex* getInternalMutex() const
    {
        return &internalMutex;
    }
};

// RAII-style lock guard for TrackedMutex
template <typename Mutex>
class TrackedLockGuard
{
private:
    Mutex& mutex;

public:
    explicit TrackedLockGuard(Mutex& mutex) : mutex(mutex)
    {
        mutex.lock();
    }

    ~TrackedLockGuard()
    {
        mutex.unlock();
    }

    TrackedLockGuard(const TrackedLockGuard&) = delete;
    TrackedLockGuard& operator=(const TrackedLockGuard&) = delete;
};

#endif // DUCKDB_MUTEX_H

