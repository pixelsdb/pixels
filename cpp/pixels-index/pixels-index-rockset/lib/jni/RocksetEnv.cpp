#include "io_pixelsdb_pixels_index_rockset_jni_RocksetEnv.h"
#include "rocksdb/pluggable_compaction.h"
#include "rocksdb/cloud/db_cloud.h"

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2025-12-22
 */
 
JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetEnv_createCloudFileSystem0(
    JNIEnv* env,
    jclass,
    jstring jbucket_name,
    jstring js3_prefix) 
{
  // 1. Check AWS env vars
  if (!getenv("AWS_ACCESS_KEY_ID") ||
      !getenv("AWS_SECRET_ACCESS_KEY") ||
      !getenv("AWS_DEFAULT_REGION")) {
    env->ThrowNew(
        env->FindClass("java/lang/IllegalStateException"),
        "Missing AWS env vars: AWS_ACCESS_KEY_ID, "
        "AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION");
    return 0;
  }

  // 2. CloudEnvOptions
  ROCKSDB_NAMESPACE::CloudEnvOptions cloud_env_options;
  cloud_env_options.credentials.InitializeSimple(
      getenv("AWS_ACCESS_KEY_ID"),
      getenv("AWS_SECRET_ACCESS_KEY"));

  auto cred_status = cloud_env_options.credentials.HasValid();
  if (!cred_status.ok()) {
    env->ThrowNew(
        env->FindClass("java/lang/SecurityException"),
        cred_status.ToString().c_str());
    return 0;
  }

  // 4. bucket / prefix
  const char* bucket_chars =
      env->GetStringUTFChars(jbucket_name, nullptr);
  const char* prefix_chars =
      env->GetStringUTFChars(js3_prefix, nullptr);

  const std::string bucket_name(bucket_chars);
  const std::string object_prefix(prefix_chars);
  const std::string region(getenv("AWS_DEFAULT_REGION"));

  env->ReleaseStringUTFChars(jbucket_name, bucket_chars);
  env->ReleaseStringUTFChars(js3_prefix, prefix_chars);

  // 4. base Env
  ROCKSDB_NAMESPACE::Env* base_env =
      ROCKSDB_NAMESPACE::Env::Default();

  cloud_env_options.src_bucket.SetBucketName(bucket_name, "");
  cloud_env_options.src_bucket.SetObjectPath(object_prefix);
  cloud_env_options.src_bucket.SetRegion(region);
  cloud_env_options.dest_bucket.SetBucketName(bucket_name, "");
  cloud_env_options.dest_bucket.SetObjectPath(object_prefix);
  cloud_env_options.dest_bucket.SetRegion(region);

  // 5. Create CloudEnv
  ROCKSDB_NAMESPACE::CloudEnv* cloud_env = nullptr;
  auto status =
      ROCKSDB_NAMESPACE::CloudEnv::NewAwsEnv(
          base_env,
          cloud_env_options, nullptr, &cloud_env);

  if (!status.ok()) {
    env->ThrowNew(
        env->FindClass("java/io/IOException"),
        status.ToString().c_str());
    return 0;
  }

  return reinterpret_cast<jlong>(cloud_env);
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetEnv_disposeInternalJni(
    JNIEnv*,
    jclass,
    jlong jenv_handle) 
{
  if (jenv_handle == 0) {
    return;
  }

  auto* env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle);

  delete env;
}
