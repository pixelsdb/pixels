//
// Created by liyu on 3/14/23.
//

#include "utils/Constants.h"

int Constants::VERSION = 1;
std::string Constants::MAGIC = "PIXELS";

int Constants::DEFAULT_HDFS_BLOCK_SIZE = 256 * 1024 * 1024;
int Constants::HDFS_BUFFER_SIZE = 8 * 1024 * 1024;
int Constants::LOCAL_BUFFER_SIZE = 8 * 1024 * 1024;
int Constants::S3_BUFFER_SIZE = 8 * 1024 * 1024;
int Constants::REDIS_BUFFER_SIZE = 8 * 1024 * 1024;
int Constants::GCS_BUFFER_SIZE = 8 * 1024 * 1024;

int Constants::MIN_REPEAT = 3;
int Constants::MAX_SCOPE = 512;
int Constants::MAX_SHORT_REPEAT_LENGTH = 10;
float Constants::DICT_KEY_SIZE_THRESHOLD = 0.1F;
int Constants::INIT_DICT_SIZE = 4096;

std::string Constants::LAYOUT_VERSION_LITERAL = "layout_version";
std::string Constants::CACHE_VERSION_LITERAL = "cache_version";
std::string Constants::CACHE_COORDINATOR_LITERAL = "coordinator";
std::string Constants::CACHE_NODE_STATUS_LITERAL = "node_";
std::string Constants::CACHE_LOCATION_LITERAL = "location_";
int Constants::MAX_BLOCK_ID_LEN = 20480;

std::string Constants::AI_LOCK_PATH_PREFIX = "/pixels_ai_lock/";

std::string Constants::LOCAL_FS_ID_KEY = "pixels_storage_local_id";

std::string Constants::LOCAL_FS_META_PREFIX = "pixels_storage_local_meta:";

std::string Constants::S3_ID_KEY = "pixels_storage_s3_id";

std::string Constants::S3_META_PREFIX = "pixels_storage_s3_meta:";

std::string Constants::MINIO_ID_KEY = "pixels_storage_minio_id";

std::string Constants::MINIO_META_PREFIX = "pixels_storage_minio_meta:";

std::string Constants::REDIS_ID_KEY = "pixels_storage_redis_id";

std::string Constants::REDIS_META_PREFIX = "pixels_storage_redis_meta:";

std::string Constants::GCS_ID_KEY = "pixels_storage_gcs_id";

std::string Constants::GCS_META_PREFIX = "pixels_storage_gcs_meta:";