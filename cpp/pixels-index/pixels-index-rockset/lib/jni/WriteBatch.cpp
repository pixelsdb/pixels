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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch.h"
#include <cassert>

#include "rocksdb/write_batch.h"
#include "rocksdb/column_family.h"

#include "rocksjni/jni_util.h"
#include "rocksjni/rocksjni.h"

using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::Slice;

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch
 * Method:    newWriteBatch0
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch_newWriteBatch0(
    JNIEnv* /* env */, jclass /* cls */)
{
    auto* wb = new WriteBatch();
    return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch_disposeInternalJni(
    JNIEnv* /* env */, jclass /* cls */, jlong handle)
{
    auto* wb = reinterpret_cast<WriteBatch*>(handle);
    assert(wb != nullptr);
    delete wb;
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch
 * Method:    putDirectJni
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch_putDirectJni(
    JNIEnv* env,
    jclass /* cls */,
    jlong jwb_handle,
    jobject jkey,
    jint jkey_offset,
    jint jkey_len,
    jobject jval,
    jint jval_offset,
    jint jval_len,
    jlong jcf_handle)
{
    auto* wb = reinterpret_cast<WriteBatch*>(jwb_handle);
    assert(wb != nullptr);

    auto* cf_handle =
        reinterpret_cast<ColumnFamilyHandle*>(jcf_handle);

    auto put = [&wb, &cf_handle](Slice& key, Slice& value)
    {
        if (cf_handle == nullptr)
        {
            wb->Put(key, value);
        }
        else
        {
            wb->Put(cf_handle, key, value);
        }
    };

    ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(
        put,
        env,
        jkey,
        jkey_offset,
        jkey_len,
        jval,
        jval_offset,
        jval_len);
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch
 * Method:    deleteJni
 * Signature: (J[BIJ)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetWriteBatch_deleteJni(
    JNIEnv* env,
    jclass /* cls */,
    jlong jwb_handle,
    jbyteArray jkey,
    jint jkey_len,
    jlong jcf_handle)
{
    auto* wb = reinterpret_cast<WriteBatch*>(jwb_handle);
    assert(wb != nullptr);

    auto* cf_handle =
        reinterpret_cast<ColumnFamilyHandle*>(jcf_handle);
    assert(cf_handle != nullptr);

    auto remove = [&wb, &cf_handle](Slice key)
    {
        return wb->Delete(cf_handle, key);
    };

    std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
        ROCKSDB_NAMESPACE::JniUtil::k_op(
            remove, env, jkey, jkey_len);

    if (status != nullptr && !status->ok())
    {
        ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    }
}