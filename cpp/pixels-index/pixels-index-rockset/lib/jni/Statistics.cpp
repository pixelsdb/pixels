#include "io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics.h"
#include <cassert>
#include <memory>

#include "rocksdb/statistics.h"

using ROCKSDB_NAMESPACE::Statistics;
using ROCKSDB_NAMESPACE::CreateDBStatistics;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics
 * Method:    newStatisticsInstance
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetStatistics_newStatisticsInstance(
    JNIEnv*, jclass) 
{
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
    JNIEnv*, jclass, jlong handle) 
{
  if (handle > 0) {
    auto* sptr_statistics =
        reinterpret_cast<std::shared_ptr<Statistics>*>(handle);
    delete sptr_statistics;  // delete shared_ptr
  }
}