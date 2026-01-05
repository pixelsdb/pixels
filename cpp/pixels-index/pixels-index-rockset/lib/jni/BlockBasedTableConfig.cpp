#include "io_pixelsdb_pixels_index_rockset_jni_RocksetBlockBasedTableConfig.h"

#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include "cplusplus_to_java_convert.h"

#include <memory>
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::NewBlockBasedTableFactory;
using ROCKSDB_NAMESPACE::FilterPolicy;
using ROCKSDB_NAMESPACE::Cache;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetBlockBasedTableConfig_newTableFactoryHandle(
    JNIEnv*,
    jclass,
    jlong jblock_cache_handle,
    jlong jfilter_policy_handle,
    jlong jblock_size,
    jboolean jwhole_key_filtering,
    jboolean jcache_index_and_filter_blocks)
{
    BlockBasedTableOptions options;
    // 1. block cache
    if (jblock_cache_handle > 0) 
    {
        auto* cache_ptr =
            reinterpret_cast<std::shared_ptr<Cache>*>(
                jblock_cache_handle);
        options.block_cache = *cache_ptr;
    }
    // 2. filter policy
    if (jfilter_policy_handle > 0) 
    {
        auto* filter_ptr =
            reinterpret_cast<std::shared_ptr<const FilterPolicy>*>(
                jfilter_policy_handle);
        options.filter_policy = *filter_ptr;
    }
    // 3. block size
    if (jblock_size > 0) 
    {
        options.block_size = static_cast<size_t>(jblock_size);
    }
    // 4. whole key filtering
    options.whole_key_filtering = static_cast<bool>(jwhole_key_filtering);
    // 5. create factory
    return GET_CPLUSPLUS_POINTER(
      ROCKSDB_NAMESPACE::NewBlockBasedTableFactory(options));
}
