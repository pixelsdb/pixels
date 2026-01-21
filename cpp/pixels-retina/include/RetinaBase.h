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

#ifndef PIXELS_RETINA_RETINABASE_H
#define PIXELS_RETINA_RETINABASE_H

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>

#ifdef ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif
#ifndef RETINA_CAPACITY
#define RETINA_CAPACITY 256
#endif

namespace pixels {

/**
 * Global atomic counter for tracked Retina objects.
 */
extern std::atomic<int64_t> g_retina_tracked_memory;
extern std::atomic<int64_t> g_retina_object_count;

/**
 * RetinaBase: A CRTP-based base class for automatic memory accounting.
 */
template <typename T> class RetinaBase {
public:
    RetinaBase() {
        g_retina_tracked_memory.fetch_add(sizeof(T), std::memory_order_relaxed);
        g_retina_object_count.fetch_add(1, std::memory_order_relaxed);
    }

    RetinaBase(const RetinaBase &) {
        g_retina_tracked_memory.fetch_add(sizeof(T), std::memory_order_relaxed);
        g_retina_object_count.fetch_add(1, std::memory_order_relaxed);
    }

    RetinaBase(RetinaBase &&) noexcept {
        g_retina_tracked_memory.fetch_add(sizeof(T), std::memory_order_relaxed);
        g_retina_object_count.fetch_add(1, std::memory_order_relaxed);
    }

    RetinaBase &operator=(const RetinaBase &) = default;
    RetinaBase &operator=(RetinaBase &&) noexcept = default;

    virtual ~RetinaBase() {
        g_retina_tracked_memory.fetch_sub(sizeof(T), std::memory_order_relaxed);
        g_retina_object_count.fetch_sub(1, std::memory_order_relaxed);
    }
};

} // namespace pixels

#endif // PIXELS_RETINA_RETINABASE_H