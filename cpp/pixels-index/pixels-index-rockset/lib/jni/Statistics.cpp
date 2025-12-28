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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics.h"
#include <cassert>
#include <memory>

#include "rocksdb/statistics.h"

using ROCKSDB_NAMESPACE::Statistics;
using ROCKSDB_NAMESPACE::CreateDBStatistics;

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics
 * Method:    newStatisticsInstance
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics_newStatisticsInstance(
    JNIEnv*, jclass) {
  auto* sptr_statistics =
      new std::shared_ptr<Statistics>(CreateDBStatistics());
  return reinterpret_cast<jlong>(sptr_statistics);
}

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics
 * Method:    disposeInternalJni
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics_disposeInternalJni(
    JNIEnv*, jclass, jlong handle) {
  if (handle > 0) {
    auto* sptr_statistics =
        reinterpret_cast<std::shared_ptr<Statistics>*>(handle);
    delete sptr_statistics;  // delete shared_ptr
  }
}