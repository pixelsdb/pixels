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
    public static final String FILE_MAGIC = "PIXELS";

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
    public static final String HEARTBEAT_COORDINATOR_LITERAL = "heartbeat_coordinator_";
    public static final String HEARTBEAT_WORKER_LITERAL = "heartbeat_worker_";
    public static final String CACHE_LOCATION_LITERAL = "cache_location_";
    public static final int MAX_BLOCK_ID_LEN = 20480;

    /*
     * Issue #649:
     * Breakdown costs into vmCost and cfCost
     */
    public static final String TRANS_CONTEXT_VM_COST_CENTS_KEY = "trans_vm_cost_cents";
    public static final String TRANS_CONTEXT_CF_COST_CENTS_KEY = "trans_cf_cost_cents";
    public static final String TRANS_CONTEXT_SCAN_BYTES_KEY = "trans_scan_bytes";

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

    public static final String CF_OUTPUT_STATE_KEY_PREFIX = "pixels_turbo_cf_output";
}
