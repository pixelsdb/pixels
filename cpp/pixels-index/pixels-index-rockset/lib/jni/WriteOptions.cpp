#include "io_pixelsdb_pixels_index_rockset_jni_RocksetWriteOptions.h"
#include <cassert>
#include "rocksdb/options.h"
#include "cplusplus_to_java_convert.h"

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

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
