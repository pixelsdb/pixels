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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetLRUCache.h"
#include <memory>

#include "rocksdb/cache.h"

using ROCKSDB_NAMESPACE::Cache;
using ROCKSDB_NAMESPACE::NewLRUCache;

JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetLRUCache_newLRUCache(
    JNIEnv* /*env*/,
    jclass /*jcls*/,
    jlong jcapacity,
    jint jnum_shard_bits,
    jboolean jstrict_capacity_limit,
    jdouble jhigh_pri_pool_ratio,
    jdouble jlow_pri_pool_ratio) {

  auto* sptr_lru_cache =
      new std::shared_ptr<Cache>(
          NewLRUCache(
              static_cast<size_t>(jcapacity),
              static_cast<int>(jnum_shard_bits),
              static_cast<bool>(jstrict_capacity_limit),
              static_cast<double>(jhigh_pri_pool_ratio),
              nullptr /* memory_allocator */,
              ROCKSDB_NAMESPACE::kDefaultToAdaptiveMutex,
              ROCKSDB_NAMESPACE::kDefaultCacheMetadataChargePolicy,
              static_cast<double>(jlow_pri_pool_ratio)));

  return reinterpret_cast<jlong>(sptr_lru_cache);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetLRUCache_disposeInternalJni(
    JNIEnv* /*env*/,
    jclass /*jcls*/,
    jlong jhandle) {

  auto* sptr_lru_cache =
      reinterpret_cast<std::shared_ptr<Cache>*>(jhandle);
  delete sptr_lru_cache;
}
