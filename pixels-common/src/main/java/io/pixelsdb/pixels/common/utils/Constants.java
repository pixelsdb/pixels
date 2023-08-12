/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.common.utils;

/**
 * @author guodong
 */
public final class Constants
{
    public static final int VERSION = 1;
    public static final String MAGIC = "PIXELS";

    public static final int DEFAULT_HDFS_BLOCK_SIZE = 256 * 1024 * 1024;
    public static final int HDFS_BUFFER_SIZE = 8 * 1024 * 1024;
    public static final int LOCAL_BUFFER_SIZE = 8 * 1024 * 1024;
    public static final int S3_BUFFER_SIZE = 8 * 1024 * 1024;
    public static final int REDIS_BUFFER_SIZE = 8 * 1024 * 1024;
    public static final int GCS_BUFFER_SIZE = 8 * 1024 * 1024;

    public static final int MIN_REPEAT = 3;
    public static final int MAX_SCOPE = 512;
    public static final int MAX_SHORT_REPEAT_LENGTH = 10;
    public static final float DICT_KEY_SIZE_THRESHOLD = 0.1F;
    public static final int INIT_DICT_SIZE = 4096;

    public static final String LAYOUT_VERSION_LITERAL = "layout_version";
    public static final String CACHE_VERSION_LITERAL = "cache_version";
    public static final String CACHE_COORDINATOR_LITERAL = "coordinator";
    public static final String CACHE_NODE_STATUS_LITERAL = "node_";
    public static final String CACHE_LOCATION_LITERAL = "location_";
    public static final int MAX_BLOCK_ID_LEN = 20480;

    /**
     * The time in seconds that a relaxed query can be postponed for execution.
     */
    public static final int RELAXED_EXECUTION_MAX_POSTPONE_SEC = 300;
    /**
     * The interval in seconds that the postponed relaxed queries are retried.
     */
    public static final int RELAXED_EXECUTION_RETRY_INTERVAL_SEC = 30;

    /**
     * Issue #108:
     * The prefix for read-write lock used in etcd auto-increment id.
     */
    public static final String AI_LOCK_PATH_PREFIX = "/pixels_ai_lock/";

    public static final String LOCAL_FS_ID_KEY = "pixels_storage_local_id";
    // the prefix for keys of local fs metadata (i.e. file path -> file id).
    public static final String LOCAL_FS_META_PREFIX = "pixels_storage_local_meta:";

    public static final String S3_ID_KEY = "pixels_storage_s3_id";
    // the prefix for keys of s3 metadata (i.e. file path -> file id).
    public static final String S3_META_PREFIX = "pixels_storage_s3_meta:";

    public static final String MINIO_ID_KEY = "pixels_storage_minio_id";
    // the prefix for keys of minio metadata (i.e. file path -> file id).
    public static final String MINIO_META_PREFIX = "pixels_storage_minio_meta:";

    public static final String REDIS_ID_KEY = "pixels_storage_redis_id";
    // the prefix for keys of redis metadata (i.e. file path -> file id).
    public static final String REDIS_META_PREFIX = "pixels_storage_redis_meta:";

    public static final String GCS_ID_KEY = "pixels_storage_gcs_id";
    // the prefix for keys of gcs metadata (i.e. file path -> file id).
    public static final String GCS_META_PREFIX = "pixels_storage_gcs_meta:";
}
