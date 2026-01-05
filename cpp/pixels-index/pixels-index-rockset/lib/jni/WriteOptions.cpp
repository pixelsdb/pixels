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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetWriteOptions.h"
#include <cassert>
#include "rocksdb/options.h"
#include "cplusplus_to_java_convert.h"

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetWriteOptions
 * Method:    newWriteOptions
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetWriteOptions_newWriteOptions(
    JNIEnv*, jclass)
{
  auto* write_options = new ROCKSDB_NAMESPACE::WriteOptions();
  return GET_CPLUSPLUS_POINTER(write_options);
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetWriteOptions
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetWriteOptions_disposeInternalJni(
    JNIEnv*, jclass, jlong jhandle)
{
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(jhandle);
  assert(write_options != nullptr);
  delete write_options;
}
