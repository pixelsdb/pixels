/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical.domain;

/**
 * @author hank
 * @create 2022-06-02
 */
public abstract class TableInfo
{
    private String tableName;

    /**
     * Whether this is a base table.
     * We don't check the existence of the base table files.
     */
    private boolean base;

    /**
     * The name of the columns to read.
     */
    private String[] columnsToRead;

    /**
     * The information of the storage endpoint.
     */
    private StorageInfo storageInfo;

    /**
     * Default constructor for Jackson.
     */
    public TableInfo() { }

    public TableInfo(String tableName, boolean base, String[] columnsToRead, StorageInfo storageInfo)
    {
        this.tableName = tableName;
        this.base = base;
        this.columnsToRead = columnsToRead;
        this.storageInfo = storageInfo;
    }

    public String getTableName()
    {
        return tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public boolean isBase()
    {
        return base;
    }

    public void setBase(boolean base)
    {
        this.base = base;
    }

    public String[] getColumnsToRead()
    {
        return columnsToRead;
    }

    public void setColumnsToRead(String[] columnsToRead)
    {
        this.columnsToRead = columnsToRead;
    }

    public StorageInfo getStorageInfo()
    {
        return storageInfo;
    }

    public void setStorageInfo(StorageInfo storageInfo)
    {
        this.storageInfo = storageInfo;
    }
}
