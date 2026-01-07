#include <jni.h>

#include <algorithm>
#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace ROCKSDB_NAMESPACE {
class JniUtil {
    public:
    /**
    * Detect if jlong overflows size_t
    *
    * @param jvalue the jlong value
    *
    * @return
    */
    inline static Status check_if_jlong_fits_size_t(const jlong& jvalue) {
        Status s = Status::OK();
        if (static_cast<uint64_t>(jvalue) > std::numeric_limits<size_t>::max()) {
        s = Status::InvalidArgument(Slice("jlong overflows 32 bit value."));
        }
        return s;
    }

        /*
    * Helper for operations on a key and value
    * for example WriteBatch->Delete
    *
    * TODO(AR) could be extended to cover returning ROCKSDB_NAMESPACE::Status
    * from `op` and used for RocksDB->Delete etc.
    */
    static void k_op_direct(std::function<void(ROCKSDB_NAMESPACE::Slice&)> op,
                            JNIEnv* env, jobject jkey, jint jkey_off,
                            jint jkey_len) {
        char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
        if (key == nullptr ||
            env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
        jclass ex = env->FindClass("java/lang/RuntimeException");
        env->ThrowNew(ex, "Invalid key argument");
        return;
        }

        key += jkey_off;

        ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);

        return op(key_slice);
    }

        /*
    * Helper for operations on a key which is a region of an array
    * Used to extract the common code from seek/seekForPrev.
    * Possible that it can be generalised from that.
    *
    * We use GetByteArrayRegion to copy the key region of the whole array into
    * a char[] We suspect this is not much slower than GetByteArrayElements,
    * which probably copies anyway.
    */
    static void k_op_region(std::function<void(ROCKSDB_NAMESPACE::Slice&)> op,
                            JNIEnv* env, jbyteArray jkey, jint jkey_off,
                            jint jkey_len) {
        const std::unique_ptr<char[]> key(new char[jkey_len]);
        if (key == nullptr) {
        jclass oom_class = env->FindClass("/lang/java/OutOfMemoryError");
        env->ThrowNew(oom_class,
                        "Memory allocation failed in RocksDB JNI function");
        return;
        }
        env->GetByteArrayRegion(jkey, jkey_off, jkey_len,
                                reinterpret_cast<jbyte*>(key.get()));
        if (env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return;
        }

        ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key.get()),
                                        jkey_len);
        op(key_slice);
    }
    /*
   * Helper for operations on a key and value
   * for example WriteBatch->Put
   *
   * TODO(AR) could be extended to cover returning ROCKSDB_NAMESPACE::Status
   * from `op` and used for RocksDB->Put etc.
   */
    static void kv_op_direct(
        std::function<void(ROCKSDB_NAMESPACE::Slice&, ROCKSDB_NAMESPACE::Slice&)>
            op,
        JNIEnv* env, jobject jkey, jint jkey_off, jint jkey_len, jobject jval,
        jint jval_off, jint jval_len) {
        char* key = reinterpret_cast<char*>(env->GetDirectBufferAddress(jkey));
        if (key == nullptr ||
            env->GetDirectBufferCapacity(jkey) < (jkey_off + jkey_len)) {
            jclass ex = env->FindClass("java/lang/RuntimeException");
            env->ThrowNew(ex, "Invalid key argument");
            return;
        }

        char* value = reinterpret_cast<char*>(env->GetDirectBufferAddress(jval));
        if (value == nullptr ||
            env->GetDirectBufferCapacity(jval) < (jval_off + jval_len)) {
            jclass ex = env->FindClass("java/lang/RuntimeException");
            env->ThrowNew(ex, "Invalid value argument");
            return;
        }

        key += jkey_off;
        value += jval_off;

        ROCKSDB_NAMESPACE::Slice key_slice(key, jkey_len);
        ROCKSDB_NAMESPACE::Slice value_slice(value, jval_len);

        op(key_slice, value_slice);
    }

    /*
   * Helper for operations on a key
   * for example WriteBatch->Delete
   *
   * TODO(AR) could be used for RocksDB->Delete etc.
   */
    static std::unique_ptr<ROCKSDB_NAMESPACE::Status> k_op(
        std::function<ROCKSDB_NAMESPACE::Status(ROCKSDB_NAMESPACE::Slice)> op,
        JNIEnv* env, jbyteArray jkey, jint jkey_len) {
        jbyte* key = env->GetByteArrayElements(jkey, nullptr);
        if (env->ExceptionCheck()) {
        // exception thrown: OutOfMemoryError
        return nullptr;
        }

        ROCKSDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

        auto status = op(key_slice);

        if (key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
        }

        return std::unique_ptr<ROCKSDB_NAMESPACE::Status>(
            new ROCKSDB_NAMESPACE::Status(status));
    }
  };
}