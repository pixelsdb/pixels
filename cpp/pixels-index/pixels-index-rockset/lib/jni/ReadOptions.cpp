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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions.h"
#include <cassert>
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::ReadOptions;

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions
 * Method:    newReadOptions
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions_newReadOptions(
    JNIEnv*, jclass) {
  auto* opts = new ReadOptions();
  return reinterpret_cast<jlong>(opts);
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions_disposeInternalJni(
    JNIEnv*, jclass, jlong handle) {
  auto* opts = reinterpret_cast<ReadOptions*>(handle);
  assert(opts != nullptr);
  delete opts;
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions
 * Method:    setPrefixSameAsStart
 * Signature: (JZ)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions_setPrefixSameAsStart(
    JNIEnv*, jclass, jlong handle, jboolean flag) {
  reinterpret_cast<ReadOptions*>(handle)->prefix_same_as_start =
      static_cast<bool>(flag);
}