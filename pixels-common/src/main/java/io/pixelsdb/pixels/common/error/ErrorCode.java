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
 * Created at: 19-4-19
 * Author: hank
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
    public static final int METADATA_ADD_COUMNS_FAILED = (ERROR_BASE_METADATA + 10);
    public static final int METADATA_UPDATE_COUMN_FAILED = (ERROR_BASE_METADATA + 11);
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
    // end error code for metadata rpc

    // begin error code for shared memory message queue
    private static final int ERROR_BASE_MQ = ERROR_BASE + 100;
    public static final int ERROR_MQ_IS_EMPTY = (ERROR_BASE_MQ + 1);
    public static final int ERROR_MQ_IS_FULL = (ERROR_BASE_MQ + 2);
    public static final int ERROR_MQ_WRITER_IS_ROLLBACK = (ERROR_BASE_MQ + 3);
    public static final int ERROR_MQ_WRITER_IS_RUNNING = (ERROR_BASE_MQ + 4);
    public static final int ERROR_MQ_READER_IS_ROLLBACK = (ERROR_BASE_MQ + 5);
    public static final int ERROR_MQ_READER_IS_RUNNING = (ERROR_BASE_MQ + 6);
    public static final int ERROR_MQ_READER_INVALID_MESSAGE_LENGTH = (ERROR_BASE_MQ + 6);
    // end error code for shared memory message queue

    // begin error code for transactions
    private static final int ERROR_TRANS = ERROR_BASE + 200;
    public static final int TRANS_LOW_WATERMARK_NOT_PUSHED = (ERROR_TRANS + 1);
    public static final int TRANS_HIGH_WATERMARK_NOT_PUSHED = (ERROR_TRANS + 2);
    public static final int TRANS_ID_NOT_EXIST = (ERROR_TRANS + 3);
    public static final int TRANS_BAD_GET_CONTEXT_REQUEST = (ERROR_TRANS + 4);
    // end error code for transactions
}
