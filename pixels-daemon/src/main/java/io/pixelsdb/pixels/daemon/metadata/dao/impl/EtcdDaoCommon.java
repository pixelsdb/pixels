/*
 * Copyright 2020 PixelsDB.
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
package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdDaoCommon
{
    // prefix + schema id
    public static final String schemaPrimaryKeyPrefix = "pixels_meta_schema_primary_";
    // prefix + schema name
    public static final String schemaNameKeyPrefix = "pixels_meta_schema_name_";
    // prefix + table id
    public static final String tablePrimaryKeyPrefix = "pixels_meta_table_primary_";
    // prefix + view id
    public static final String viewPrimaryKeyPrefix = "pixels_meta_view_primary_";
    // prefix + schema id + table name
    public static final String tableSchemaNameKeyPrefix = "pixels_meta_table_schema_name_";
    // prefix + schema id + view name
    public static final String viewSchemaNameKeyPrefix = "pixels_meta_view_schema_name_";
    // prefix + column id
    public static final String columnPrimaryKeyPrefix = "pixels_meta_column_primary_";
    // prefix + table id + column name
    public static final String columnTableNameKeyPrefix = "pixels_meta_column_table_name_";
    // prefix + layout id
    public static final String layoutPrimaryKeyPrefix = "pixels_meta_layout_primary_";
    // prefix + table id + layout id
    public static final String layoutTableIdKeyPrefix = "pixels_meta_layout_table_";

    // used for generating ids for schemas, tables, layouts, and columns.
    //public static final String schemaIdLockPath = "/pixels_meta/schema_id_lock";
    public static final String schemaIdKey = "pixels_meta_schema_id";
    //public static final String tableIdLockPath = "/pixels_meta/table_id_lock";
    public static final String tableIdKey = "pixels_meta_table_id";
    //public static final String viewIdLockPath = "/pixels_meta/view_id_lock";
    public static final String viewIdKey = "pixels_meta_view_id";
    //public static final String layoutIdLockPath = "/pixels_meta/layout_id_lock";
    public static final String layoutIdKey = "pixels_meta_layout_id";
    //public static final String columnIdLockPath = "/pixels_meta/column_id_lock";
    public static final String columnIdKey = "pixels_meta_column_id";

    static
    {
        InitId(schemaIdKey);
        InitId(tableIdKey);
        InitId(viewIdKey);
        InitId(layoutIdKey);
        InitId(columnIdKey);
    }
}
