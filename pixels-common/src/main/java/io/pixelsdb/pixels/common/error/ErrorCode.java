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
package io.pixelsdb.pixels.common.error;

/**
 * @author hank
 * @create 2019-04-19
 */
public class ErrorCode
{
    public static final int SUCCESS = 0;
    private static final int ERROR_BASE = 10000;

    // begin error code for metadata rpc
    private static final int ERROR_BASE_METADATA = ERROR_BASE;
    public static final int METADATA_SCHEMA_NOT_FOUND = (ERROR_BASE_METADATA + 1);
    public static final int METADATA_TABLE_NOT_FOUND = (ERROR_BASE_METADATA + 2);
    public static final int METADATA_LAYOUT_NOT_FOUND = (ERROR_BASE_METADATA + 3);
    public static final int METADATA_COLUMN_NOT_FOUND = (ERROR_BASE_METADATA + 4);
    public static final int METADATA_LAYOUT_DUPLICATED = (ERROR_BASE_METADATA + 5);
    public static final int METADATA_SCHEMA_EXIST = (ERROR_BASE_METADATA + 6);
    public static final int METADATA_TABLE_EXIST = (ERROR_BASE_METADATA + 7);
    public static final int METADATA_DELETE_SCHEMA_FAILED = (ERROR_BASE_METADATA + 8);
    public static final int METADATA_DELETE_TABLE_FAILED = (ERROR_BASE_METADATA + 9);
    public static final int METADATA_ADD_COLUMNS_FAILED = (ERROR_BASE_METADATA + 10);
    public static final int METADATA_UPDATE_COLUMN_FAILED = (ERROR_BASE_METADATA + 11);
    public static final int METADATA_UPDATE_LAYOUT_FAILED = (ERROR_BASE_METADATA + 12);
    public static final int METADATA_ADD_LAYOUT_FAILED = (ERROR_BASE_METADATA + 13);
    public static final int METADATA_ADD_SCHEMA_FAILED = (ERROR_BASE_METADATA + 14);
    public static final int METADATA_ADD_TABLE_FAILED = (ERROR_BASE_METADATA + 15);
    public static final int METADATA_VIEW_NOT_FOUND = (ERROR_BASE_METADATA + 16);
    public static final int METADATA_VIEW_EXIST = (ERROR_BASE_METADATA + 17);
    public static final int METADATA_ADD_VIEW_FAILED = (ERROR_BASE_METADATA + 18);
    public static final int METADATA_DELETE_VIEW_FAILED = (ERROR_BASE_METADATA + 19);
    public static final int METADATA_UNKNOWN_DATA_TYPE = (ERROR_BASE_METADATA + 20);
    public static final int METADATA_UPDATE_TABLE_FAILED = (ERROR_BASE_METADATA + 21);
    public static final int METADATA_ADD_PATH_FAILED = (ERROR_BASE_METADATA + 22);
    public static final int METADATA_GET_PATHS_FAILED = (ERROR_BASE_METADATA + 23);
    public static final int METADATA_UPDATE_PATH_FAILED = (ERROR_BASE_METADATA + 24);
    public static final int METADATA_DELETE_PATHS_FAILED = (ERROR_BASE_METADATA + 25);
    public static final int METADATA_ADD_PEER_FAILED = (ERROR_BASE_METADATA + 26);
    public static final int METADATA_GET_PEER_FAILED = (ERROR_BASE_METADATA + 27);
    public static final int METADATA_UPDATE_PEER_FAILED = (ERROR_BASE_METADATA + 28);
    public static final int METADATA_DELETE_PEER_FAILED = (ERROR_BASE_METADATA + 29);
    public static final int METADATA_ADD_PEER_PATH_FAILED = (ERROR_BASE_METADATA + 30);
    public static final int METADATA_GET_PEER_PATHS_FAILED = (ERROR_BASE_METADATA + 31);
    public static final int METADATA_UPDATE_PEER_PATH_FAILED = (ERROR_BASE_METADATA + 32);
    public static final int METADATA_DELETE_PEER_PATHS_FAILED = (ERROR_BASE_METADATA + 33);
    public static final int METADATA_ADD_SCHEMA_VERSION_FAILED = (ERROR_BASE_METADATA + 34);
    public static final int METADATA_ADD_RANGE_INDEX_FAILED = (ERROR_BASE_METADATA + 35);
    public static final int METADATA_RANGE_INDEX_NOT_FOUND = (ERROR_BASE_METADATA + 36);
    public static final int METADATA_UPDATE_RANGE_INDEX_FAILED = (ERROR_BASE_METADATA + 37);
    public static final int METADATA_DELETE_RANGE_INDEX_FAILED = (ERROR_BASE_METADATA + 38);
    public static final int METADATA_ADD_RANGE_FAILED = (ERROR_BASE_METADATA + 39);
    public static final int METADATA_GET_RANGES_FAILED = (ERROR_BASE_METADATA + 40);
    public static final int METADATA_RANGE_NOT_FOUNT = (ERROR_BASE_METADATA + 41);
    public static final int METADATA_DELETE_RANGE_FAILED = (ERROR_BASE_METADATA + 42);
    public static final int METADATA_ADD_FILE_FAILED = (ERROR_BASE_METADATA + 43);
    public static final int METADATA_GET_FILES_FAILED = (ERROR_BASE_METADATA + 44);
    public static final int METADATA_GET_FILE_ID_FAILED = (ERROR_BASE_METADATA + 45);
    public static final int METADATA_UPDATE_FILE_FAILED = (ERROR_BASE_METADATA + 46);
    public static final int METADATA_DELETE_FILES_FAILED = (ERROR_BASE_METADATA + 47);
    public static final int METADATA_ADD_RETINA_BUFFER_FAILED = (ERROR_BASE_METADATA + 48);
    // end error code for metadata rpc

    // begin error code for shared memory message queue
    private static final int ERROR_BASE_MQ = ERROR_BASE + 100;
    public static final int ERROR_MQ_IS_EMPTY = (ERROR_BASE_MQ + 1);
    public static final int ERROR_MQ_IS_FULL = (ERROR_BASE_MQ + 2);
    public static final int ERROR_MQ_WRITER_IS_ROLLBACK = (ERROR_BASE_MQ + 3);
    public static final int ERROR_MQ_WRITER_IS_RUNNING = (ERROR_BASE_MQ + 4);
    public static final int ERROR_MQ_READER_IS_ROLLBACK = (ERROR_BASE_MQ + 5);
    public static final int ERROR_MQ_READER_IS_RUNNING = (ERROR_BASE_MQ + 6);
    public static final int ERROR_MQ_READER_INVALID_MESSAGE_LENGTH = (ERROR_BASE_MQ + 7);
    // end error code for shared memory message queue

    // begin error code for transactions
    private static final int ERROR_TRANS = ERROR_BASE + 200;
    public static final int TRANS_LOW_WATERMARK_NOT_PUSHED = (ERROR_TRANS + 1);
    public static final int TRANS_HIGH_WATERMARK_NOT_PUSHED = (ERROR_TRANS + 2);
    public static final int TRANS_ID_NOT_EXIST = (ERROR_TRANS + 3);
    public static final int TRANS_CONTEXT_NOT_FOUND = (ERROR_TRANS + 4);
    public static final int TRANS_COMMIT_FAILED = (ERROR_TRANS + 5);
    public static final int TRANS_ROLLBACK_FAILED = (ERROR_TRANS + 6);
    public static final int TRANS_GENERATE_ID_OR_TS_FAILED = (ERROR_TRANS + 7);

    // begin error code for query schedule
    private static final int ERROR_QUERY_SCHEDULE = ERROR_BASE + 300;
    public static final int QUERY_SCHEDULE_DEQUEUE_FAILED = (ERROR_QUERY_SCHEDULE + 1);

    // begin error code for query server
    private static final int ERROR_QUERY_SERVER = ERROR_BASE + 400;
    public static final int QUERY_SERVER_PENDING_INTERRUPTED = (ERROR_QUERY_SERVER + 1);
    public static final int QUERY_SERVER_EXECUTE_FAILED = (ERROR_QUERY_SERVER + 2);
    public static final int QUERY_SERVER_NOT_SUPPORTED = (ERROR_QUERY_SERVER + 3);
    public static final int QUERY_SERVER_BAD_REQUEST = (ERROR_QUERY_SERVER + 4);
    public static final int QUERY_SERVER_QUERY_NOT_FINISHED = (ERROR_QUERY_SERVER + 5);
    public static final int QUERY_SERVER_QUERY_RESULT_NOT_FOUND = (ERROR_QUERY_SERVER + 6);

    // begin error code for pixels-turbo worker coordinate service
    private static final int ERROR_WORKER_COORDINATE = ERROR_BASE + 500;
    public static final int WORKER_COORDINATE_NO_DOWNSTREAM = ERROR_WORKER_COORDINATE + 1;
    public static final int WORKER_COORDINATE_WORKER_NOT_FOUND = ERROR_WORKER_COORDINATE + 2;
    public static final int WORKER_COORDINATE_END_OF_TASKS = ERROR_WORKER_COORDINATE + 3;
    public static final int WORKER_COORDINATE_LEASE_EXPIRED = ERROR_WORKER_COORDINATE + 4;

    // end error code for transactions
}
