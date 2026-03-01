#include "io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions.h"
#include <cassert>
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::ReadOptions;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

/*
 * Class:     io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions
 * Method:    newReadOptions
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetReadOptions_newReadOptions(
    JNIEnv*, jclass) 
{
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
    JNIEnv*, jclass, jlong handle) 
{
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
    JNIEnv*, jclass, jlong handle, jboolean flag) 
{
  reinterpret_cast<ReadOptions*>(handle)->prefix_same_as_start =
      static_cast<bool>(flag);
}