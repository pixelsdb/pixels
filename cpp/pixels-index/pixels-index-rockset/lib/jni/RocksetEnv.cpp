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
#include "io_pixelsdb_pixels_index_rockset_jni_RocksetEnv.h"

JNIEXPORT jlong JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetEnv_createCloudFileSystem0(
    JNIEnv* env,
    jclass,
    jstring jbucket_name,
    jstring js3_prefix) {

  // 1. 检查 AWS 环境变量
  if (!getenv("AWS_ACCESS_KEY_ID") ||
      !getenv("AWS_SECRET_ACCESS_KEY") ||
      !getenv("AWS_DEFAULT_REGION")) {
    env->ThrowNew(
        env->FindClass("java/lang/IllegalStateException"),
        "Missing AWS env vars: AWS_ACCESS_KEY_ID, "
        "AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION");
    return 0;
  }

  // 2. AWS SDK
  Aws::SDKOptions aws_options;
  Aws::InitAPI(aws_options);

  // 3. CloudFileSystemOptions
  ROCKSDB_NAMESPACE::CloudFileSystemOptions cfs_options;
  cfs_options.credentials.InitializeSimple(
      getenv("AWS_ACCESS_KEY_ID"),
      getenv("AWS_SECRET_ACCESS_KEY"));

  if (!cfs_options.credentials.HasValid().ok()) {
    env->ThrowNew(
        env->FindClass("java/lang/SecurityException"),
        "Invalid AWS credentials");
    return 0;
  }

  // 4. bucket / prefix
  const char* bucket_chars =
      env->GetStringUTFChars(jbucket_name, nullptr);
  const char* prefix_chars =
      env->GetStringUTFChars(js3_prefix, nullptr);

  cfs_options.src_bucket.SetBucketName(bucket_chars);
  cfs_options.src_bucket.SetObjectPath(prefix_chars);
  cfs_options.src_bucket.SetRegion(
      getenv("AWS_DEFAULT_REGION"));

  env->ReleaseStringUTFChars(jbucket_name, bucket_chars);
  env->ReleaseStringUTFChars(js3_prefix, prefix_chars);

  // 5. base Env
  ROCKSDB_NAMESPACE::Env* base_env =
      ROCKSDB_NAMESPACE::Env::Default();

  // 6. Create CloudFileSystem
  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> base_fs =
      base_env->GetFileSystem();

  ROCKSDB_NAMESPACE::CloudFileSystem* cloud_fs = nullptr;
  auto status =
      ROCKSDB_NAMESPACE::CloudFileSystemEnv::NewAwsFileSystem(
          base_fs, cfs_options, nullptr, &cloud_fs);

  if (!status.ok()) {
    env->ThrowNew(
        env->FindClass("java/io/IOException"),
        status.ToString().c_str());
    return 0;
  }

  // 7. Composite Env
  std::shared_ptr<ROCKSDB_NAMESPACE::FileSystem> fs(cloud_fs);
  std::unique_ptr<ROCKSDB_NAMESPACE::Env> cloud_env =
      ROCKSDB_NAMESPACE::CloudFileSystemEnv::NewCompositeEnv(
          base_env, std::move(fs));

  return reinterpret_cast<jlong>(cloud_env.release());
}

JNIEXPORT void JNICALL
Java_io_pixelsdb_pixels_index_rockset_jni_RocksetEnv_disposeInternalJni(
    JNIEnv*,
    jclass,
    jlong jenv_handle) {

  if (jenv_handle == 0) {
    return;
  }

  auto* env =
      reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jenv_handle);

  delete env;
}

