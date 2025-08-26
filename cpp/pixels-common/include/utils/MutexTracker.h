/*
/* @author whz
/* @create 8/23/25.
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
    // 存储锁与持有者线程ID的映射
    std::unordered_map<const std::mutex*, std::thread::id> mutex_owners;
    // 保护映射表的内部锁
    std::mutex internal_mutex;

public:
    // 获取锁的持有者线程ID，若未被持有则返回默认ID
    std::thread::id get_owner(const std::mutex* mutex) {
        std::lock_guard<std::mutex> lock(internal_mutex);
        auto it = mutex_owners.find(mutex);
        if (it != mutex_owners.end()) {
            return it->second;
        }
        return std::thread::id(); // 未被持有
    }

    // 记录锁被持有
    void lock_acquired(const std::mutex* mutex) {
        std::lock_guard<std::mutex> lock(internal_mutex);
        auto current_thread = std::this_thread::get_id();

        // 检查是否重复获取（防止递归锁问题）
        auto it = mutex_owners.find(mutex);
        if (it != mutex_owners.end()) {
            if (it->second == current_thread) {
                // 同一线程重复获取非递归锁，可能导致死锁
                throw std::runtime_error("同一线程重复获取非递归锁");
            }
        }

        mutex_owners[mutex] = current_thread;
    }

    // 记录锁被释放
    void lock_released(const std::mutex* mutex) {
        std::lock_guard<std::mutex> lock(internal_mutex);
        mutex_owners.erase(mutex);
    }

    // 打印锁的持有者信息
    void print_owner(const std::mutex* mutex, const std::string& mutex_name) {
        std::thread::id owner = get_owner(mutex);
        if (owner == std::thread::id()) {
            std::cout << mutex_name << " 未被任何线程持有" << std::endl;
        } else {
            std::cout << mutex_name << " 被线程 " << std::hash<std::thread::id>{}(owner)
                      << " 持有" << std::endl;
        }
    }
};


// 带跟踪功能的互斥锁包装器
class TrackedMutex {
private:
    std::mutex internal_mutex;
    std::string name;
    MutexTracker g_mutex_tracker;

public:
    TrackedMutex(const std::string& name,MutexTracker &g_mutex_tracker) : name(name) {}

    // 加锁并记录持有者
    void lock() {
        internal_mutex.lock();
        g_mutex_tracker.lock_acquired(&internal_mutex);
    }

    // 解锁并清除持有者记录
    void unlock() {
        g_mutex_tracker.lock_released(&internal_mutex);
        internal_mutex.unlock();
    }

    // 尝试加锁（成功则记录持有者）
    bool try_lock() {
        if (internal_mutex.try_lock()) {
            g_mutex_tracker.lock_acquired(&internal_mutex);
            return true;
        }
        return false;
    }

    // 获取锁名称
    const std::string& get_name() const { return name; }

    // 获取内部互斥锁指针（用于跟踪）
    const std::mutex* get_internal_mutex() const {
        return &internal_mutex;
    }
};

// 与TrackedMutex配合使用的RAII锁管理
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

    // 禁止拷贝
    TrackedLockGuard(const TrackedLockGuard&) = delete;
    TrackedLockGuard& operator=(const TrackedLockGuard&) = delete;
};


#endif // DUCKDB_MUTEX_H
