/*
 * Copyright 2024 PixelsDB.
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
package io.pixelsdb.pixels.common.metadata.domain;

import java.util.Date;

/**
 * The simple layout to be observable in the 'show create table' result.
 *
 * @author hank
 * @create 2024-09-08
 */
public class SimpleLayout
{
    private long version;
    private Date createAt;
    private Permission permission;
    private String[] orderedPaths;
    private String[] compactPaths;

    public SimpleLayout() { }

    public SimpleLayout(long version, Date createAt, Permission permission,
                        String[] orderedPaths, String[] compactPaths)
    {
        this.version = version;
        this.createAt = createAt;
        this.permission = permission;
        this.orderedPaths = orderedPaths;
        this.compactPaths = compactPaths;
    }

    public long getVersion()
    {
        return version;
    }

    public void setVersion(long version)
    {
        this.version = version;
    }

    public Date getCreateAt()
    {
        return createAt;
    }

    public void setCreateAt(Date createAt)
    {
        this.createAt = createAt;
    }

    public Permission getPermission()
    {
        return permission;
    }

    public void setPermission(Permission permission)
    {
        this.permission = permission;
    }

    public String[] getOrderedPaths()
    {
        return orderedPaths;
    }

    public void setOrderedPaths(String[] orderedPaths)
    {
        this.orderedPaths = orderedPaths;
    }

    public String[] getCompactPaths()
    {
        return compactPaths;
    }

    public void setCompactPaths(String[] compactPaths)
    {
        this.compactPaths = compactPaths;
    }
}
