#include "io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyHandle.h"
#include <rocksdb/db.h>
#include <cassert>

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetColumnFamilyHandle_disposeInternalJni(
    JNIEnv*,
    jclass,
    jlong jhandle)
{
    auto* cfh =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jhandle);

    assert(cfh != nullptr);
    delete cfh;
}