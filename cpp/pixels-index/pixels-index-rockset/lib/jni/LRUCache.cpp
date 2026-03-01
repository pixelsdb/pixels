#include "io_pixelsdb_pixels_index_rockset_jni_RocksetLRUCache.h"
#include <memory>

#include "rocksdb/cache.h"

using ROCKSDB_NAMESPACE::Cache;
using ROCKSDB_NAMESPACE::NewLRUCache;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetLRUCache_newLRUCache(
    JNIEnv* /*env*/,
    jclass /*jcls*/,
    jlong jcapacity,
    jint jnum_shard_bits,
    jboolean jstrict_capacity_limit,
    jdouble jhigh_pri_pool_ratio,
    jdouble jlow_pri_pool_ratio) 
{
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
    jlong jhandle) 
{
  auto* sptr_lru_cache =
      reinterpret_cast<std::shared_ptr<Cache>*>(jhandle);
  delete sptr_lru_cache;
}
