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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetBlockBasedTableConfig.h"

#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <memory>
using ROCKSDB_NAMESPACE::BlockBasedTableOptions;
using ROCKSDB_NAMESPACE::NewBlockBasedTableFactory;
using ROCKSDB_NAMESPACE::TableFactory;
using ROCKSDB_NAMESPACE::FilterPolicy;
using ROCKSDB_NAMESPACE::Cache;

/**
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
    if (jblock_cache_handle > 0) {
        auto* cache_ptr =
            reinterpret_cast<std::shared_ptr<Cache>*>(
                jblock_cache_handle);
        options.block_cache = *cache_ptr;
    }

    // 2. filter policy
    if (jfilter_policy_handle > 0) {
        auto* filter_ptr =
            reinterpret_cast<std::shared_ptr<const FilterPolicy>*>(
                jfilter_policy_handle);
        options.filter_policy = *filter_ptr;
    }

    // 3. block size
    if (jblock_size > 0) {
        options.block_size = static_cast<size_t>(jblock_size);
    }

    // 4. whole key filtering
    options.whole_key_filtering = static_cast<bool>(jwhole_key_filtering);


    // 5. 创建 factory
    auto factory =
        NewBlockBasedTableFactory(options);

    // 6. 返回 shared_ptr 句柄（与 RocksDB JNI 一致）
    return reinterpret_cast<jlong>(
        new std::shared_ptr<TableFactory>(factory));
}
