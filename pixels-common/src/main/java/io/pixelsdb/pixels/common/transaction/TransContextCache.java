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

import io.pixelsdb.pixels.daemon.TransProto;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This is only a cache of the contexts of the transactions handled in this process.
 * The context is this cache may be not strictly consistent with the global transaction context.
 * Thus, the result returned by this cache may be inaccurate.
 *
 * @create 2022-02-20
 * @update 2023-05-02 turn this class from trans context to the local trans context cache.
 * @author hank
 */
public class TransContextCache
{
    private static TransContextCache instance;

    public static TransContextCache Instance()
    {
        if (instance == null)
        {
            instance = new TransContextCache();
        }
        return instance;
    }

    private final Map<Long, TransContext> transIdToContext = new HashMap<>();;
    private volatile int readOnlyConcurrency;

    private TransContextCache() { }

    protected synchronized void addTransContext(TransContext context)
    {
        requireNonNull(context, "transaction context is null");
        this.transIdToContext.put(context.getTransId(), context);
        if (context.isReadOnly())
        {
            this.readOnlyConcurrency++;
        }
    }

    protected synchronized void setTransCommit(long transId)
    {
        TransContext context = this.transIdToContext.remove(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.COMMIT);
            if (context.isReadOnly())
            {
                this.readOnlyConcurrency--;
            }
        }
    }

    protected synchronized void setTransRollback(long transId)
    {
        TransContext context = this.transIdToContext.remove(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.ROLLBACK);
            if (context.isReadOnly())
            {
                this.readOnlyConcurrency--;
            }
        }
    }

    public synchronized boolean isTerminated(long transId)
    {
        TransContext info =  this.transIdToContext.get(transId);
        return info == null || info.getStatus() == TransProto.TransStatus.COMMIT ||
                info.getStatus() == TransProto.TransStatus.ROLLBACK;
    }

    /**
     * @return the number of concurrent queries at this moment.
     */
    public synchronized int getQueryConcurrency()
    {
        return this.readOnlyConcurrency;
    }
}
