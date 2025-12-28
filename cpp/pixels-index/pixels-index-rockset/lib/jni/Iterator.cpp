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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetIterator.h"
#include <cassert>

#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"

#include "rocksjni/jni_util.h"
#include "rocksjni/rocksjni.h"

using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Slice;

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetIterator
 * Method:    seekDirect0Jni
 * Signature: (JLjava/nio/ByteBuffer;II)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetIterator_seekDirect0Jni(
    JNIEnv* env,
    jclass /* cls */,
    jlong handle,
    jobject jtarget,
    jint jtarget_off,
    jint jtarget_len)
{
    auto* it = reinterpret_cast<Iterator*>(handle);
    assert(it != nullptr);

    auto seek = [&it](Slice& target_slice)
    {
        it->Seek(target_slice);
    };

    ROCKSDB_NAMESPACE::JniUtil::k_op_direct(
        seek, env, jtarget, jtarget_off, jtarget_len);
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetIterator
 * Method:    seekByteArray0Jni
 * Signature: (J[BII)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetIterator_seekByteArray0Jni(
    JNIEnv* env,
    jclass /* cls */,
    jlong handle,
    jbyteArray jtarget,
    jint jtarget_off,
    jint jtarget_len)
{
    auto* it = reinterpret_cast<Iterator*>(handle);
    assert(it != nullptr);

    auto seek = [&it](Slice& target_slice)
    {
        it->Seek(target_slice);
    };

    ROCKSDB_NAMESPACE::JniUtil::k_op_region(
        seek, env, jtarget, jtarget_off, jtarget_len);
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetIterator
 * Method:    next0
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetIterator_next0(
    JNIEnv* /* env */,
    jclass /* cls */,
    jlong handle)
{
    reinterpret_cast<Iterator*>(handle)->Next();
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetIterator_isValid0(
    JNIEnv* /* env */,
    jclass /* cls */,
    jlong handle)
{
    return reinterpret_cast<Iterator*>(handle)->Valid();
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetIterator
 * Method:    key0
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetIterator_key0(
    JNIEnv* env,
    jclass /* cls */,
    jlong handle)
{
    auto* it = reinterpret_cast<Iterator*>(handle);
    assert(it != nullptr);

    Slice key_slice = it->key();

    jbyteArray jkey =
        env->NewByteArray(static_cast<jsize>(key_slice.size()));
    if (jkey == nullptr)
    {
        // OutOfMemoryError already thrown
        return nullptr;
    }

    env->SetByteArrayRegion(
        jkey,
        0,
        static_cast<jsize>(key_slice.size()),
        reinterpret_cast<const jbyte*>(key_slice.data()));

    return jkey;
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetIterator
 * Method:    value0
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetIterator_value0(
    JNIEnv* env,
    jclass /* cls */,
    jlong handle)
{
    auto* it = reinterpret_cast<Iterator*>(handle);
    assert(it != nullptr);

    Slice value_slice = it->value();

    jbyteArray jvalue =
        env->NewByteArray(static_cast<jsize>(value_slice.size()));
    if (jvalue == nullptr)
    {
        // OutOfMemoryError already thrown
        return nullptr;
    }

    env->SetByteArrayRegion(
        jvalue,
        0,
        static_cast<jsize>(value_slice.size()),
        reinterpret_cast<const jbyte*>(value_slice.data()));

    return jvalue;
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetIterator
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetIterator_disposeInternalJni(
    JNIEnv* /* env */,
    jclass /* cls */,
    jlong handle)
{
    auto* it = reinterpret_cast<Iterator*>(handle);
    assert(it != nullptr);
    delete it;
}
