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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include <cassert>
#include <string>

using ROCKSDB_NAMESPACE::DBOptions;
using ROCKSDB_NAMESPACE::Statistics;

/**
 * @author Rolland1944
 * @create 2025-12-22
 */


JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_newDBOptions(
    JNIEnv*, jclass)
{
    auto* dbop = new DBOptions();
    return reinterpret_cast<jlong>(dbop);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_disposeInternalJni(
    JNIEnv*, jclass, jlong jhandle)
{
    auto* dbo = reinterpret_cast<DBOptions*>(jhandle);
    assert(dbo != nullptr);
    delete dbo;
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetCreateIfMissing(
    JNIEnv*, jclass, jlong jhandle, jboolean flag) {
    reinterpret_cast<DBOptions*>(jhandle)->create_if_missing = flag;
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetCreateMissingColumnFamilies(
    JNIEnv*, jclass, jlong jhandle, jboolean flag) {
    reinterpret_cast<DBOptions*>(jhandle)->create_missing_column_families = flag;
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetMaxBackgroundFlushes(
    JNIEnv*, jclass, jlong jhandle, jint num) {
    reinterpret_cast<DBOptions*>(jhandle)->max_background_flushes = static_cast<int>(num);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetMaxBackgroundCompactions(
    JNIEnv*, jclass, jlong jhandle, jint num) {
    reinterpret_cast<DBOptions*>(jhandle)->max_background_compactions = static_cast<int>(num);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetMaxSubcompactions(
    JNIEnv*, jclass, jlong jhandle, jint num) {
    reinterpret_cast<DBOptions*>(jhandle)->max_subcompactions = static_cast<int>(num);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetMaxOpenFiles(
    JNIEnv*, jclass, jlong jhandle, jint num) {
    reinterpret_cast<DBOptions*>(jhandle)->max_open_files = static_cast<int>(num);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetStatistics(
    JNIEnv*, jclass, jlong jhandle, jlong stats_handle) {
    auto* options = reinterpret_cast<DBOptions*>(jhandle);
    auto* stats = reinterpret_cast<std::shared_ptr<Statistics>*>(stats_handle);
    options->statistics = *stats;
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetStatsDumpPeriodSec(
    JNIEnv*, jclass, jlong jhandle, jint interval) {
    reinterpret_cast<DBOptions*>(jhandle)->stats_dump_period_sec = static_cast<int>(interval);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDBOptions_nativeSetDbLogDir(
    JNIEnv* env, jclass, jlong jhandle, jstring jpath) {
    auto* options = reinterpret_cast<DBOptions*>(jhandle);
    const char* cpath = env->GetStringUTFChars(jpath, nullptr);
    options->db_log_dir = std::string(cpath);
    env->ReleaseStringUTFChars(jpath, cpath);
}
