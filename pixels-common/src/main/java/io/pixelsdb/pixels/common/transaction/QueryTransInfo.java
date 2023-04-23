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
package io.pixelsdb.pixels.common.transaction;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * @create 2022-02-20
 * @author hank
 */
public class QueryTransInfo
{
    private long queryId;
    private long queryTimestamp;
    private AtomicReference<Status> queryStatus;
    private Properties queryProperties;

    public enum Status
    {
        PENDING, COMMIT, ROLLBACK
    }

    public QueryTransInfo(long queryId, long queryTimestamp)
    {
        this.queryId = queryId;
        this.queryTimestamp = queryTimestamp;
        this.queryStatus = new AtomicReference<>(Status.PENDING);
        this.queryProperties = new Properties();
    }

    public long getQueryId()
    {
        return queryId;
    }

    public long getQueryTimestamp()
    {
        return queryTimestamp;
    }

    public Status getQueryStatus()
    {
        return queryStatus.get();
    }

    public void setQueryStatus(Status status)
    {
        this.queryStatus.set(status);
    }

    public Properties getQueryProperties()
    {
        return this.queryProperties;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("queryTimestamp", queryTimestamp)
                .add("queryStatus", queryStatus.get())
                .toString();
    }
}
