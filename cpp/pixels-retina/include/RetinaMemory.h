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

#ifndef PIXELS_RETINA_RETINAMEMORY_H
#define PIXELS_RETINA_RETINAMEMORY_H

#include <cstddef>
#include <new>
#include <atomic>
#include <cstdlib>

#ifdef ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace pixels {

    struct MemoryStats {
        static inline std::atomic<std::size_t> allocated_bytes{0};
        static inline std::atomic<std::size_t> allocated_count{0};
        static inline std::atomic<std::size_t> freed_count{0};
    };

    inline void* alloc(std::size_t size) {
        // 分配内存
#ifdef ENABLE_JEMALLOC
        void* p = pixels_malloc(size);
#else
        void* p = std::malloc(size);
#endif
        if (!p) throw std::bad_alloc();

        // 统计逻辑
        MemoryStats::allocated_bytes.fetch_add(size, std::memory_order_relaxed);
        MemoryStats::allocated_count.fetch_add(1, std::memory_order_relaxed);

        return p;
    }

    inline void free(void* p) noexcept {
        if (!p) return;

        // 增加释放计数
        MemoryStats::freed_count.fetch_add(1, std::memory_order_relaxed);

#ifdef ENABLE_JEMALLOC
        pixels_free(p);
#else
        std::free(p);
#endif
    }

    /**
     * 获取当前内存状态的辅助函数
     */
    inline std::size_t get_total_allocated() { return MemoryStats::allocated_bytes.load(); }
    inline std::size_t get_live_objects() {
        return MemoryStats::allocated_count.load() - MemoryStats::freed_count.load();
    }

    class RetinaBase {
    public:
        static void* operator new(std::size_t sz) { return alloc(sz); }
        static void operator delete(void* p) noexcept { free(p); }

        static void* operator new[](std::size_t sz) { return alloc(sz); }
        static void operator delete[](void* p) noexcept { free(p); }

        static void operator delete(void* p, std::size_t) noexcept { free(p); }
        static void operator delete[](void* p, std::size_t) noexcept { free(p); }

        // Placement new/delete
        static void* operator new(std::size_t, void* p) noexcept { return p; }
        static void operator delete(void*, void*) noexcept {}
    };

    template <typename T>
    struct Allocator {
        using value_type = T;
        Allocator() = default;
        template <class U> Allocator(const Allocator<U>&) {}

        T* allocate(std::size_t n) {
            // 这里会调用上面带统计逻辑的 ::pixels::alloc
            return static_cast<T*>(::pixels::alloc(n * sizeof(T)));
        }

        void deallocate(T* p, std::size_t n) {
            // 这里会调用上面带统计逻辑的 ::pixels::free
            ::pixels::free(p);
        }
    };

} // namespace pixels

#endif // PIXELS_RETINA_RETINAMEMORY_H
