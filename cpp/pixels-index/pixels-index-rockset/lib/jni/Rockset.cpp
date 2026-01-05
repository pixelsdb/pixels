#include "io_pixelsdb_pixels_index_rockset_jni_RocksetDB.h"
#include <vector>
#include <string>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/options.h"
#include "rocksdb/db.h"
#include "portal.h"
#include "cplusplus_to_java_convert.h"

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */

JNIEXPORT jlongArray JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_open(
    JNIEnv* env,
    jclass,
    jlong cloud_env_ptr,
    jlong joptions,
    jstring jdb_path,
    jobjectArray jcf_names,
    jlongArray jcf_options_handles) 
{
  // 1. Options*
  auto* options =
      reinterpret_cast<ROCKSDB_NAMESPACE::Options*>(joptions);
  assert(options != nullptr);
  options->env = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(cloud_env_ptr);

  // 2. db path
  const char* db_path_chars =
      env->GetStringUTFChars(jdb_path, nullptr);
  std::string db_path(db_path_chars);
  env->ReleaseStringUTFChars(jdb_path, db_path_chars);

  // 3. column families
  jsize cf_count = env->GetArrayLength(jcf_names);
  jsize opt_count = env->GetArrayLength(jcf_options_handles);
  assert(cf_count == opt_count);

  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyDescriptor> cf_descs;
  cf_descs.reserve(cf_count);

  jlong* cf_opts =
      env->GetLongArrayElements(jcf_options_handles, nullptr);

  for (jsize i = 0; i < cf_count; ++i) {
    auto jname =
        (jbyteArray)env->GetObjectArrayElement(jcf_names, i);

    jsize len = env->GetArrayLength(jname);
    std::string cf_name(len, '\0');
    env->GetByteArrayRegion(
        jname, 0, len,
        reinterpret_cast<jbyte*>(&cf_name[0]));

    auto* cf_options =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyOptions*>(
            cf_opts[i]);

    cf_descs.emplace_back(cf_name, *cf_options);

    env->DeleteLocalRef(jname);
  }

  env->ReleaseLongArrayElements(
      jcf_options_handles, cf_opts, JNI_ABORT);

  // 4. open DBCloud
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> handles;
  ROCKSDB_NAMESPACE::DBCloud* db = nullptr;

  auto status = ROCKSDB_NAMESPACE::DBCloud::Open(
      *options,
      db_path,
      cf_descs,
      "" /* persistent_cache_path */,
      0  /* persistent_cache_size_gb */,
      &handles,
      &db,
      false /* read_only */
  );

  if (!status.ok()) {
    return nullptr;
  }

  // 5. prepare return value
  jlongArray result =
      env->NewLongArray(handles.size() + 1);

  std::vector<jlong> values;
  values.reserve(handles.size() + 1);

  values.push_back(reinterpret_cast<jlong>(db));
  for (auto* h : handles) {
    values.push_back(reinterpret_cast<jlong>(h));
  }

  env->SetLongArrayRegion(
      result, 0, values.size(), values.data());

  return result;
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_closeDatabase(
    JNIEnv*, jclass, jlong jdb) 
{
  auto* db =
      reinterpret_cast<ROCKSDB_NAMESPACE::DBCloud*>(jdb);
  if (db == nullptr) {
    return;
  }

  delete db;
}

JNIEXPORT jobject JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_listColumnFamilies0(
    JNIEnv* env, jclass, jstring jdb_path)
{
  const char* path_chars = env->GetStringUTFChars(jdb_path, nullptr);
  if (path_chars == nullptr) {
    return nullptr; // OOM
  }
  std::string db_path(path_chars);
  env->ReleaseStringUTFChars(jdb_path, path_chars);
  ROCKSDB_NAMESPACE::Options options;
  std::vector<std::string> column_families;
  ROCKSDB_NAMESPACE::Status st =
      ROCKSDB_NAMESPACE::DBCloud::ListColumnFamilies(
          options, db_path, &column_families);

  if (!st.ok()) {
    jclass ex = env->FindClass("java/lang/RuntimeException");
    env->ThrowNew(ex, st.ToString().c_str());
    return nullptr;
  }

  jclass array_list_clz = env->FindClass("java/util/ArrayList");
  jmethodID array_list_ctor =
      env->GetMethodID(array_list_clz, "<init>", "(I)V");
  jmethodID array_list_add =
      env->GetMethodID(array_list_clz, "add", "(Ljava/lang/Object;)Z");

  jobject jlist =
      env->NewObject(array_list_clz, array_list_ctor,
                     static_cast<jint>(column_families.size()));

  for (const auto& cf : column_families) {
    jbyteArray jbytes = env->NewByteArray(cf.size());
    env->SetByteArrayRegion(
        jbytes, 0, cf.size(),
        reinterpret_cast<const jbyte*>(cf.data()));
    env->CallBooleanMethod(jlist, array_list_add, jbytes);
    env->DeleteLocalRef(jbytes);
  }

  return jlist;
}

JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_createColumnFamily0(
    JNIEnv* env, jobject jdb, jlong jhandle, jbyteArray jcf_name)
{
  auto* db = reinterpret_cast<ROCKSDB_NAMESPACE::DB*>(jhandle);
  if (db == nullptr) {
    jclass ex = env->FindClass("java/lang/RuntimeException");
    env->ThrowNew(ex, "DB is closed");
    return reinterpret_cast<jlong>(nullptr);
  }

  jsize len = env->GetArrayLength(jcf_name);
  std::string cf_name;
  cf_name.resize(len);
  env->GetByteArrayRegion(
      jcf_name, 0, len,
      reinterpret_cast<jbyte*>(&cf_name[0]));

  ROCKSDB_NAMESPACE::ColumnFamilyOptions cf_options;

  ROCKSDB_NAMESPACE::ColumnFamilyHandle* cf_handle = nullptr;
  ROCKSDB_NAMESPACE::Status st =
      db->CreateColumnFamily(cf_options, cf_name, &cf_handle);

  if (!st.ok()) {
    jclass ex = env->FindClass("java/lang/RuntimeException");
    env->ThrowNew(ex, st.ToString().c_str());
    return reinterpret_cast<jlong>(nullptr);
  }

  auto* sptr =
      new std::shared_ptr<ROCKSDB_NAMESPACE::ColumnFamilyHandle>(cf_handle);

  jclass cf_handle_clz =
      env->FindClass(
          "io/pixelsdb/pixels/index/rockset/jni/RocksetColumnFamilyHandle");

  jmethodID ctor =
      env->GetMethodID(cf_handle_clz, "<init>", "(J)V");

  return GET_CPLUSPLUS_POINTER(cf_handle);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_put(
    JNIEnv* env,
    jclass,
    jlong db_handle,
    jlong cf_handle,
    jbyteArray jkey,
    jint koff,
    jint klen,
    jbyteArray jval,
    jint voff,
    jint vlen,
    jlong write_opt_handle) 
{
    auto* db =
        reinterpret_cast<ROCKSDB_NAMESPACE::DBCloud*>(db_handle);
    auto* cf =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(cf_handle);
    auto* wo =
        reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(write_opt_handle);

    jbyte* k = env->GetByteArrayElements(jkey, nullptr);
    jbyte* v = env->GetByteArrayElements(jval, nullptr);

    ROCKSDB_NAMESPACE::Slice key(
        reinterpret_cast<char*>(k + koff), klen);
    ROCKSDB_NAMESPACE::Slice val(
        reinterpret_cast<char*>(v + voff), vlen);

    auto status = db->Put(*wo, cf, key, val);

    env->ReleaseByteArrayElements(jkey, k, JNI_ABORT);
    env->ReleaseByteArrayElements(jval, v, JNI_ABORT);

    if (!status.ok()) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"),
                      status.ToString().c_str());
    }
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_putDirect(
    JNIEnv* env,
    jclass,
    jlong jdb_handle,
    jlong jcf_handle,
    jobject jkey,
    jint jkey_off,
    jint jkey_len,
    jobject jval,
    jint jval_off,
    jint jval_len,
    jlong jwrite_options_handle) 
{
  auto* db =
      reinterpret_cast<ROCKSDB_NAMESPACE::DBCloud*>(jdb_handle);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto* write_options =
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(
          jwrite_options_handle);

  if (db == nullptr || write_options == nullptr) {
    env->ThrowNew(env->FindClass("java/lang/IllegalStateException"),
                  "DB or WriteOptions is null");
    return;
  }

  auto put =
      [&env, &db, &cf_handle, &write_options](
          ROCKSDB_NAMESPACE::Slice& key,
          ROCKSDB_NAMESPACE::Slice& value) {

        ROCKSDB_NAMESPACE::Status s;
        if (cf_handle == nullptr) {
          s = db->Put(*write_options, key, value);
        } else {
          s = db->Put(*write_options, cf_handle, key, value);
        }

        if (!s.ok()) {
          env->ThrowNew(env->FindClass("java/lang/RuntimeException"),
                        s.ToString().c_str());
        }
      };

  ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(
      put,
      env,
      jkey,
      jkey_off,
      jkey_len,
      jval,
      jval_off,
      jval_len);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_write0(
    JNIEnv* env,
    jclass,
    jlong db_handle,
    jlong write_opt_handle,
    jlong batch_handle) 
{
    auto* db =
        reinterpret_cast<ROCKSDB_NAMESPACE::DBCloud*>(db_handle);
    auto* wo =
        reinterpret_cast<ROCKSDB_NAMESPACE::WriteOptions*>(write_opt_handle);
    auto* batch =
        reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(batch_handle);

    auto status = db->Write(*wo, batch);

    if (!status.ok()) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"),
                      status.ToString().c_str());
    }
}

JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetDB_iterator(
    JNIEnv* env,
    jclass,
    jlong db_handle,
    jlong cf_handle,
    jlong read_opt_handle) 
{
    auto* db =
        reinterpret_cast<ROCKSDB_NAMESPACE::DBCloud*>(db_handle);
    auto* cf =
        reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(cf_handle);
    auto* ro =
        reinterpret_cast<ROCKSDB_NAMESPACE::ReadOptions*>(read_opt_handle);

    if (!db || !cf || !ro) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"),
                      "Null handle in iterator()");
        return 0;
    }

    auto* it = db->NewIterator(*ro, cf);
    return reinterpret_cast<jlong>(it);
}

