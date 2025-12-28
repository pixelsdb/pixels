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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions.h"
#include <rocksdb/options.h>

using ROCKSDB_NAMESPACE::ColumnFamilyOptions;
using ROCKSDB_NAMESPACE::CompressionType;
using ROCKSDB_NAMESPACE::CompactionStyle;

/**
 * @author Rolland1944
 * @create 2025-12-22
 */


JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_newColumnFamilyOptions(
    JNIEnv* /*env*/,
    jclass /*jcls*/) 
{
    auto* op = new ROCKSDB_NAMESPACE::ColumnFamilyOptions();
    return reinterpret_cast<jlong>(op);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetWriteBufferSize(
    JNIEnv* env, jclass, jlong jhandle, jlong jwrite_buffer_size) 
{
    if (jwrite_buffer_size < 0 || jwrite_buffer_size > static_cast<jlong>(SIZE_MAX)) {
        jclass exc = env->FindClass("java/lang/IllegalArgumentException");
        env->ThrowNew(exc, "write_buffer_size out of range");
        return;
    }
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->write_buffer_size = static_cast<size_t>(jwrite_buffer_size);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetMaxWriteBufferNumber(
    JNIEnv*, jclass, jlong jhandle, jint jmax_write_buffer_number) 
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->max_write_buffer_number = static_cast<int>(jmax_write_buffer_number);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetMinWriteBufferNumberToMerge(
    JNIEnv*, jclass, jlong jhandle, jint jmin_write_buffer_number_to_merge) 
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->min_write_buffer_number_to_merge = static_cast<int>(jmin_write_buffer_number_to_merge);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetMemtablePrefixBloomSizeRatio(
    JNIEnv*, jclass, jlong jhandle, jdouble jmemtable_prefix_bloom_size_ratio) 
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->memtable_prefix_bloom_size_ratio = static_cast<double>(jmemtable_prefix_bloom_size_ratio);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetTableFactory(
    JNIEnv*, jclass, jlong jhandle, jlong jtable_factory_handle) 
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    auto* table_factory = reinterpret_cast<ROCKSDB_NAMESPACE::TableFactory*>(jtable_factory_handle);
    options->table_factory.reset(table_factory);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetLevel0FileNumCompactionTrigger(
    JNIEnv*, jclass, jlong jhandle, jint jlevel0_file_num_compaction_trigger) 
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->level0_file_num_compaction_trigger = static_cast<int>(jlevel0_file_num_compaction_trigger);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetMaxBytesForLevelBase(
    JNIEnv*, jclass, jlong jhandle, jlong jmax_bytes_for_level_base) 
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->max_bytes_for_level_base = static_cast<int64_t>(jmax_bytes_for_level_base);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetMaxBytesForLevelMultiplier(
    JNIEnv*, jclass, jlong jhandle, jdouble jmultiplier)
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->max_bytes_for_level_multiplier = static_cast<double>(jmultiplier);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetTargetFileSizeBase(
    JNIEnv*, jclass, jlong jhandle, jlong jtarget_file_size_base)
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->target_file_size_base = static_cast<size_t>(jtarget_file_size_base);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetTargetFileSizeMultiplier(
    JNIEnv*, jclass, jlong jhandle, jint jtarget_file_size_multiplier)
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->target_file_size_multiplier = static_cast<int>(jtarget_file_size_multiplier);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompression_type)
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->compression = static_cast<CompressionType>(jcompression_type);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetBottommostCompressionType(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompression_type)
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->bottommost_compression = static_cast<CompressionType>(jcompression_type);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_nativeSetCompactionStyle(
    JNIEnv*, jclass, jlong jhandle, jbyte jcompaction_style)
{
    auto* options = reinterpret_cast<ColumnFamilyOptions*>(jhandle);
    options->compaction_style = static_cast<CompactionStyle>(jcompaction_style);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyOptions_disposeInternalJni(JNIEnv*, jclass,
                                                             jlong handle) {
  auto* cfo = reinterpret_cast<ColumnFamilyOptions*>(handle);
  assert(cfo != nullptr);
  delete cfo;
}



