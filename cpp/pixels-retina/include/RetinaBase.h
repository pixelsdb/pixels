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

#include <cstddef>
#include <new>
#include <cstdlib>
#include <atomic>

#ifdef ENABLE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace pixels {

 /**
  * Global atomic counter for tracked Retina objects.
  * Must be defined in a .cpp file: std::atomic<int64_t> pixels::g_retina_tracked_memory{0};
  */
 extern std::atomic<int64_t> g_retina_tracked_memory;
 extern std::atomic<int64_t> g_retina_object_count;
 /**
  * RetinaBase: A CRTP-based base class for automatic memory accounting.
  * Usage: class MyObject : public RetinaBase<MyObject> { ... };
  */
 template <typename T>
 class RetinaBase {
 public:
  // Constructor: increment the global counter by the size of the derived class
  RetinaBase() {
   g_retina_tracked_memory.fetch_add(sizeof(T), std::memory_order_relaxed);
   g_retina_object_count.fetch_add(1, std::memory_order_relaxed);
  }

  // Copy constructor: handle object duplication
  RetinaBase(const RetinaBase&) {
   g_retina_tracked_memory.fetch_add(sizeof(T), std::memory_order_relaxed);
   g_retina_object_count.fetch_add(1, std::memory_order_relaxed);
  }

  // Move constructor: memory usage stays the same for the new instance
  RetinaBase(RetinaBase&&) noexcept {
   g_retina_tracked_memory.fetch_add(sizeof(T), std::memory_order_relaxed);
   g_retina_object_count.fetch_add(1, std::memory_order_relaxed);
  }

  RetinaBase& operator=(const RetinaBase&) = default;
  RetinaBase& operator=(RetinaBase&&) noexcept = default;

  // Virtual destructor: decrement the counter when the object is destroyed
  virtual ~RetinaBase() {
   g_retina_tracked_memory.fetch_sub(sizeof(T), std::memory_order_relaxed);
   g_retina_object_count.fetch_sub(1, std::memory_order_relaxed);
  }
 };

} // namespace pixels

#endif // PIXELS_RETINA_RETINABASE_H