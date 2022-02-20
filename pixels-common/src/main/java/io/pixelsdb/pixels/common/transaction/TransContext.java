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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * <P>
 *     The context of the transactions in Pixels.
 * </P>
 *
 * Created at: 20/02/2022
 * Author: hank
 */
public class TransContext
{
    private static TransContext instance;

    public static TransContext Instance()
    {
        if (instance == null)
        {
            instance = new TransContext();
        }
        return instance;
    }

    private Map<Long, QueryTransInfo> queryTransContext;

    private TransContext()
    {
        this.queryTransContext = new ConcurrentHashMap<>();
    }

    public void beginQuery(QueryTransInfo info)
    {
        requireNonNull(info, "query transaction info is null");
        checkArgument(info.getQueryStatus() == QueryTransInfo.Status.PENDING);
        this.queryTransContext.put(info.getQueryId(), info);
    }

    public void commitQuery(long queryId)
    {
        QueryTransInfo info = this.queryTransContext.remove(queryId);
        if (info != null)
        {
            info.setQueryStatus(QueryTransInfo.Status.COMMIT);
        }
    }

    public void rollbackQuery(long queryId)
    {
        QueryTransInfo info = this.queryTransContext.remove(queryId);
        if (info != null)
        {
            info.setQueryStatus(QueryTransInfo.Status.ROLLBACK);
        }
    }

    public QueryTransInfo getQueryTransInfo(long queryId)
    {
        return this.queryTransContext.get(queryId);
    }
}
