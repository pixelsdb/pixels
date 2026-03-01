#include "io_pixelsdb_pixels_index_rockset_jni_Filter.h"

#include "rocksdb/filter_policy.h"
#include <memory>

using ROCKSDB_NAMESPACE::FilterPolicy;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_Filter_disposeInternalJni(
    JNIEnv* /*env*/,
    jclass /*jcls*/,
    jlong jhandle)
{
    // jhandle is a pointer to std::shared_ptr<const FilterPolicy>
    auto* handle =
        reinterpret_cast<std::shared_ptr<const FilterPolicy>*>(jhandle);

    delete handle;  // decrement refcount and free wrapper
}