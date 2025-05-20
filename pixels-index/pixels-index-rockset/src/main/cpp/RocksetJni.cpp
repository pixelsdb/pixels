#include "io_pixelsdb_pixels_index_rocksdb_RocksetIndex.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/convenience.h"
#include <iostream>
#include <aws/core/Aws.h>
#include <string>

using ROCKSDB_NAMESPACE::CloudFileSystem;
using ROCKSDB_NAMESPACE::CloudFileSystemEnv;
using ROCKSDB_NAMESPACE::CloudFileSystemOptions;
using ROCKSDB_NAMESPACE::DBCloud;
using ROCKSDB_NAMESPACE::Env;
using ROCKSDB_NAMESPACE::FileSystem;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;

// Helper function to convert jstring to std::string
std::string jstring_to_string(JNIEnv* env, jstring jstr) {
    const char* cstr = env->GetStringUTFChars(jstr, nullptr);
    std::string str(cstr);
    env->ReleaseStringUTFChars(jstr, cstr);
    return str;
}

// Test if set the environment
bool check_env_vars(JNIEnv* env) {
    if (!getenv("AWS_ACCESS_KEY_ID") || !getenv("AWS_SECRET_ACCESS_KEY") || !getenv("AWS_DEFAULT_REGION")) {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"),
            "Missing required environment variables: "
            "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION");
        return false;
    }
    return true;
}

/*
 * Class:     io_pixelsdb_pixels_index_rocksdb_RocksetInde
 * Method:    CreateCloudFileSystem0
 * Signature: (Ljava/lang/String;Ljava/lang/String;[J)J
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_index_rocksdb_RocksetIndex_CreateCloudFileSystem0(
    JNIEnv* env, jobject obj, 
    jstring bucket_name, jstring s3_prefix,
    jlongArray base_env_ptr_out) {

    if (!check_env_vars(env)) return 0;

    Aws::SDKOptions options;
    Aws::InitAPI(options);

    // 初始化 CloudFileSystem 配置
    CloudFileSystemOptions cloud_fs_options;
    cloud_fs_options.credentials.InitializeSimple(
        getenv("AWS_ACCESS_KEY_ID"), 
        getenv("AWS_SECRET_ACCESS_KEY"));
    
    if (!cloud_fs_options.credentials.HasValid().ok()) {
        env->ThrowNew(env->FindClass("java/lang/SecurityException"),
            "Invalid AWS credentials in environment variables");
        return 0;
    }

    // 设置 S3 存储桶和路径
    std::string bucket = jstring_to_string(env, bucket_name);
    std::string prefix = jstring_to_string(env, s3_prefix);
    cloud_fs_options.src_bucket.SetBucketName(bucket);
    cloud_fs_options.src_bucket.SetObjectPath(prefix);
    cloud_fs_options.src_bucket.SetRegion("cn-north-1");

    // 创建基础环境
    Env* base_env = Env::Default();
    
    // 创建 CloudFileSystem
    std::shared_ptr<FileSystem> base_fs = base_env->GetFileSystem();
    CloudFileSystem* cfs;
    std::cout << "开始创建CloudFileSystem"<< std::endl;
    Status s = CloudFileSystemEnv::NewAwsFileSystem(base_fs, cloud_fs_options, nullptr, &cfs);
    if (!s.ok()) {
        env->ThrowNew(env->FindClass("java/io/IOException"),
            "Failed to create CloudFileSystem. Check S3 permissions and bucket name.");
        return 0;
    }

    // 返回 CompositeEnv 和 base_env
    std::shared_ptr<FileSystem> fs(cfs);
    std::unique_ptr<Env> cloud_env = CloudFileSystemEnv::NewCompositeEnv(base_env, std::move(fs));
    Env* raw_env_ptr = cloud_env.release();

    // 输出 base_env 指针
    jlong* out_elements = env->GetLongArrayElements(base_env_ptr_out, nullptr);
    out_elements[0] = reinterpret_cast<jlong>(base_env);
    env->ReleaseLongArrayElements(base_env_ptr_out, out_elements, 0);
    std::cout << "完成创建CloudFileSystem"<< std::endl;

    // return reinterpret_cast<jlong>(cloud_env.get());
    return reinterpret_cast<jlong>(raw_env_ptr);
}

/*
 * Class:     io_pixelsdb_pixels_index_rocksdb_RocksetIndex
 * Method:    OpenDBCloud0
 * Signature: (JLjava/lang/String;Ljava/lang/String;JZ)J
 */
JNIEXPORT jlong JNICALL Java_io_pixelsdb_pixels_index_rocksdb_RocksetIndex_OpenDBCloud0(
    JNIEnv* env, jobject obj, 
    jlong cloud_env_ptr, jstring local_db_path,
    jstring persistent_cache_path, jlong persistent_cache_size_gb,
    jboolean read_only) {
    
    // Convert Java strings
    std::string db_path = jstring_to_string(env, local_db_path);
    std::string cache_path = jstring_to_string(env, persistent_cache_path);

    // Configure options
    Options options;
    options.env = reinterpret_cast<Env*>(cloud_env_ptr);
    options.create_if_missing = true;
    options.best_efforts_recovery = true;  // 忽略缺失文件
    options.paranoid_checks = false;       // 关闭严格校验

    // Open DBCloud
    DBCloud* dbcloud = nullptr;
    Status s = DBCloud::Open(
        options,
        db_path,
        cache_path,
        static_cast<uint64_t>(persistent_cache_size_gb),
        &dbcloud,
        static_cast<bool>(read_only)
    );

    if (!s.ok()) {
        std::cout << "打开DBCloud失败: " << s.ToString() << std::endl;
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"),
                     "Failed to open DBCloud");
        return 0;
    }

    return reinterpret_cast<jlong>(dbcloud);
}

/*
 * Class:     io_pixelsdb_pixels_index_rocksdb_RocksetIndex
 * Method:    DBput0
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_index_rocksdb_RocksetIndex_DBput0(
    JNIEnv* env, jobject obj, 
    jlong db_ptr, jbyteArray key, jbyteArray value) {
    
    DBCloud* db = reinterpret_cast<DBCloud*>(db_ptr);
    jbyte* key_data = env->GetByteArrayElements(key, nullptr);
    jsize key_len = env->GetArrayLength(key);
    jbyte* value_data = env->GetByteArrayElements(value, nullptr);
    jsize value_len = env->GetArrayLength(value);

    Slice key_slice(reinterpret_cast<char*>(key_data), key_len);
    Slice value_slice(reinterpret_cast<char*>(value_data), value_len);

    Status s = db->Put(WriteOptions(), key_slice, value_slice);

    env->ReleaseByteArrayElements(key, key_data, JNI_ABORT);
    env->ReleaseByteArrayElements(value, value_data, JNI_ABORT);

    if (!s.ok()) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"),
                     "Put operation failed");
    }
}

/*
 * Class:     io_pixelsdb_pixels_index_rocksdb_RocksetIndex
 * Method:    DBget0
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_io_pixelsdb_pixels_index_rocksdb_RocksetIndex_DBget0(
    JNIEnv* env, jobject obj, 
    jlong db_ptr, jbyteArray key) {
    
    DBCloud* db = reinterpret_cast<DBCloud*>(db_ptr);
    jbyte* key_data = env->GetByteArrayElements(key, nullptr);
    jsize key_len = env->GetArrayLength(key);
    Slice key_slice(reinterpret_cast<char*>(key_data), key_len);

    std::string value;
    Status s = db->Get(ReadOptions(), key_slice, &value);

    env->ReleaseByteArrayElements(key, key_data, JNI_ABORT);

    if (s.ok()) {
        jbyteArray result = env->NewByteArray(value.size());
        env->SetByteArrayRegion(result, 0, value.size(), 
                               reinterpret_cast<const jbyte*>(value.data()));
        return result;
    } else if (s.IsNotFound()) {
        return nullptr;
    } else {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"),
                     "Get operation failed");
        return nullptr;
    }
}

/*
 * Class:     io_pixelsdb_pixels_index_rocksdb_RocksetIndex
 * Method:    DBdelete0
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_index_rocksdb_RocksetIndex_DBdelete0(
    JNIEnv* env, jobject obj, 
    jlong db_ptr, jbyteArray key) {
    
    DBCloud* db = reinterpret_cast<DBCloud*>(db_ptr);
    jbyte* key_data = env->GetByteArrayElements(key, nullptr);
    jsize key_len = env->GetArrayLength(key);
    Slice key_slice(reinterpret_cast<char*>(key_data), key_len);

    Status s = db->Delete(WriteOptions(), key_slice);

    env->ReleaseByteArrayElements(key, key_data, JNI_ABORT);

    if (!s.ok()) {
        env->ThrowNew(env->FindClass("java/lang/RuntimeException"),
                     "Delete operation failed");
    }
}

/*
 * Class:     io_pixelsdb_pixels_index_rocksdb_RocksetIndex
 * Method:    CloseDB0
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_io_pixelsdb_pixels_index_rocksdb_RocksetIndex_CloseDB0(
    JNIEnv* env, jobject obj, 
    jlong db_ptr, jlong base_env_ptr, jlong cloud_env_ptr) {
    
    DBCloud* db = reinterpret_cast<DBCloud*>(db_ptr);
    Env* base_env = reinterpret_cast<Env*>(base_env_ptr);
    Env* cloud_env = reinterpret_cast<Env*>(cloud_env_ptr);

     // 关键修复：停止所有后台任务
    rocksdb::CancelAllBackgroundWork(db, true); // true=等待任务完成

    Status s = db->Close(); // 或 db->SyncClose()（如果存在）
    if (!s.ok()) {
        std::cerr << "Close failed: " << s.ToString() << std::endl;
    }

    delete cloud_env;
    delete base_env;
    delete db;
}