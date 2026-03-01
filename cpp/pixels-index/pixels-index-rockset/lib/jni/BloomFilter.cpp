#include "io_pixelsdb_pixels_index_rockset_jni_RocksetBloomFilter.h"
#include <rocksdb/filter_policy.h>
#include <memory>

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetBloomFilter_createNewBloomFilter(
    JNIEnv*,
    jclass,
    jdouble bits_per_key)
{
    auto* sptr_filter =
        new std::shared_ptr<const ROCKSDB_NAMESPACE::FilterPolicy>(
            ROCKSDB_NAMESPACE::NewBloomFilterPolicy(bits_per_key));

    return reinterpret_cast<jlong>(sptr_filter);
}
