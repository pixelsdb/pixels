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
    // begin error code for metadata rpc
    public static final int METADATA_SCHEMA_NOT_FOUND = 10000;
    public static final int METADATA_TABLE_NOT_FOUND = 10001;
    public static final int METADATA_LAYOUT_NOT_FOUND = 10002;
    public static final int METADATA_COLUMN_NOT_FOUND = 10003;
    public static final int METADATA_LAYOUT_DUPLICATED = 10004;
    public static final int METADATA_SCHEMA_EXIST = 10005;
    public static final int METADATA_TABLE_EXIST = 10006;
    public static final int METADATA_DELETE_SCHEMA_FAILED = 10007;
    public static final int METADATA_DELETE_TABLE_FAILED = 10008;
    public static final int METADATA_ADD_COUMNS_FAILED = 10009;
    public static final int METADATA_UPDATE_COUMN_FAILED = 10010;
    public static final int METADATA_UPDATE_LAYOUT_FAILED = 10010;
    public static final int METADATA_ADD_LAYOUT_FAILED = 10010;
    // end error code for metadata rpc
}
