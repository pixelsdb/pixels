//
// Created by liyu on 3/14/23.
//

#ifndef PIXELS_CONSTANTS_H
#define PIXELS_CONSTANTS_H

#include <string>
class Constants {
public:
    static int VERSION;
    static std::string MAGIC;

    static int DEFAULT_HDFS_BLOCK_SIZE;
    static int HDFS_BUFFER_SIZE;
    static int LOCAL_BUFFER_SIZE;
    static int S3_BUFFER_SIZE;
    static int REDIS_BUFFER_SIZE;
    static int GCS_BUFFER_SIZE;

    static int MIN_REPEAT;
    static int MAX_SCOPE;
    static int MAX_SHORT_REPEAT_LENGTH;
    static float DICT_KEY_SIZE_THRESHOLD;
    static int INIT_DICT_SIZE;

    static std::string LAYOUT_VERSION_LITERAL;
    static std::string CACHE_VERSION_LITERAL;
    static std::string CACHE_COORDINATOR_LITERAL;
    static std::string CACHE_NODE_STATUS_LITERAL;
    static std::string CACHE_LOCATION_LITERAL;
    static int MAX_BLOCK_ID_LEN;

    /**
     * Issue #108:
     * The prefix for read-write lock used in etcd auto-increment id.
     */
    static std::string AI_LOCK_PATH_PREFIX;

    static std::string LOCAL_FS_ID_KEY;
    // the prefix for keys of local fs metadata (i.e. file path -> file id).
    static std::string LOCAL_FS_META_PREFIX;

    static std::string S3_ID_KEY;
    // the prefix for keys of s3 metadata (i.e. file path -> file id).
    static std::string S3_META_PREFIX;

    static std::string MINIO_ID_KEY;
    // the prefix for keys of minio metadata (i.e. file path -> file id).
    static std::string MINIO_META_PREFIX;

    static std::string REDIS_ID_KEY;
    // the prefix for keys of redis metadata (i.e. file path -> file id).
    static std::string REDIS_META_PREFIX;

    static std::string GCS_ID_KEY;
    // the prefix for keys of gcs metadata (i.e. file path -> file id).
    static std::string GCS_META_PREFIX;
};
#endif //PIXELS_CONSTANTS_H
