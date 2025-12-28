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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetBloomFilter.h"
#include <rocksdb/filter_policy.h>
#include <memory>

/**
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
