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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * This is only a cache of the contexts of the transactions handled in this process.
 * The context in this cache may be not strictly consistent with the global transaction context.
 * Thus, the result returned by this cache may be inaccurate.
 *
 * @create 2022-02-20
 * @update 2023-05-02 turn this class from trans context to the local trans context cache.
 * @update 2025-10-05 remove synchronized from all methods to improve performance.
 * @author hank
 */
public class TransContextCache
{
    private static final class InstanceHolder
    {
        private static final TransContextCache instance = new TransContextCache();
    }

    public static TransContextCache Instance()
    {
        return InstanceHolder.instance;
    }

    private final Map<Long, TransContext> transIdToContext = new ConcurrentHashMap<>();
    private final AtomicInteger readOnlyConcurrency = new AtomicInteger(0);

    private TransContextCache() { }

    protected void addTransContext(TransContext context)
    {
        requireNonNull(context, "transaction context is null");
        this.transIdToContext.put(context.getTransId(), context);
        if (context.isReadOnly())
        {
            this.readOnlyConcurrency.getAndIncrement();
        }
    }

    protected void setTransCommit(long transId)
    {
        TransContext context = this.transIdToContext.remove(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.COMMIT);
            if (context.isReadOnly())
            {
                this.readOnlyConcurrency.getAndDecrement();
            }
        }
    }

    protected void setTransRollback(long transId)
    {
        TransContext context = this.transIdToContext.remove(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.ROLLBACK);
            if (context.isReadOnly())
            {
                this.readOnlyConcurrency.getAndDecrement();
            }
        }
    }

    public boolean isTerminated(long transId)
    {
        TransContext info =  this.transIdToContext.get(transId);
        return info == null || info.getStatus() == TransProto.TransStatus.COMMIT ||
                info.getStatus() == TransProto.TransStatus.ROLLBACK;
    }

    /**
     * @return the number of concurrent queries at this moment.
     */
    public int getQueryConcurrency()
    {
        return this.readOnlyConcurrency.get();
    }
}
