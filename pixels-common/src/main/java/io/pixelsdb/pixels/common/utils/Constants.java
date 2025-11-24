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
 * @author guodong, hank
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
    public static final int S3QS_BUFFER_SIZE = 8 * 1024 * 1024;
    public static final int HTTP_STREAM_BUFFER_SIZE = 8 * 1024 * 1024;
    public static final int STREAM_READER_RG_BUFFER_SIZE = 1024 * 1024;
    public static final int STREAM_READER_RG_FOOTER_BUFFER_SIZE = 1024;

    public static final int MIN_REPEAT = 3;
    public static final int MAX_SCOPE = 512;
    public static final int MAX_SHORT_REPEAT_LENGTH = 10;
    public static final float DICT_KEY_SIZE_THRESHOLD = 0.1F;
    public static final int INIT_DICT_SIZE = 4096;
    public static final int MAX_STREAM_RETRY_COUNT = 100;
    public static final long STREAM_DELAY_MS = 1000;

    public static final String LAYOUT_VERSION_LITERAL = "layout_version";
    public static final String CACHE_VERSION_LITERAL = "cache_version";
    public static final String CACHE_LOCATION_LITERAL = "cache_location_";
    public static final int HOSTNAME_INDEX_IN_CACHE_LOCATION_LITERAL = 3;
    public static final String HEARTBEAT_COORDINATOR_LITERAL = "heartbeat_coordinator_";
    public static final String HEARTBEAT_WORKER_LITERAL = "heartbeat_worker_";
    public static final String HEARTBEAT_RETINA_LITERAL = "heartbeat_retina_";
    public static final String CACHE_EXPAND_OR_SHRINK_LITERAL = "cache_expand_or_shrink"; // expand or shrink;1:expand, 2:shrink

    public static final String PARTITION_OPERATOR_NAME = "partition";
    public static final String PARTITION_JOIN_OPERATOR_NAME = "partition_join";
    public static final String BROADCAST_JOIN_OPERATOR_NAME = "broadcast_join";

    /*
     * Issue #649:
     * Breakdown costs into vmCost and cfCost
     */
    public static final String TRANS_CONTEXT_VM_COST_CENTS_KEY = "trans_vm_cost_cents";
    public static final String TRANS_CONTEXT_CF_COST_CENTS_KEY = "trans_cf_cost_cents";
    public static final String TRANS_CONTEXT_SCAN_BYTES_KEY = "trans_scan_bytes";
    public static final String TRANS_HIGH_WATERMARK_KEY = "trans_high_watermark";
    public static final String TRANS_LOW_WATERMARK_KEY = "trans_low_watermark";
    public static final int TRANS_WATERMARKS_CHECKPOINT_PERIOD_SEC = 10;

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
    public static final String AI_LOCK_PATH_PREFIX = "/ai_lock/";
    // Issue #1099: increase from 1000 to 10000 to improve transaction-begin throughput
    public static final long AI_DEFAULT_STEP = 10000;
    public static final String AI_TRANS_ID_KEY = "trans_id";
    public static final String AI_ROW_ID_PREFIX = "row_id_";

    public static final String CF_OUTPUT_STATE_KEY_PREFIX = "pixels_turbo_cf_output";
}
