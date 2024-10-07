/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.daemon.transaction;

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.daemon.TransProto;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * This context manager of the transactions.
 *
 * @update 2023-05-02
 * @author hank
 */
public class TransContextManager
{
    private static TransContextManager instance;

    protected static TransContextManager Instance()
    {
        if (instance == null)
        {
            instance = new TransContextManager();
        }
        return instance;
    }

    private final Map<Long, TransContext> transIdToContext = new HashMap<>();
    /**
     * Two different transactions will not have the same transaction id, so we can store the running transactions
     * using a tree set and sort the transaction contexts by transaction timestamp and id.
     */
    private final Set<TransContext> runningReadOnlyTrans = new TreeSet<>();
    private final Set<TransContext> runningWriteTrans = new TreeSet<>();

    private final Map<String, Long> traceIdToTransId = new HashMap<>();
    private final Map<Long, String> transIdToTraceId = new HashMap<>();
    private final AtomicInteger readOnlyConcurrency = new AtomicInteger(0);

    private TransContextManager() { }

    public synchronized void addTransContext(TransContext context)
    {
        requireNonNull(context, "transaction context is null");
        this.transIdToContext.put(context.getTransId(), context);
        if (context.isReadOnly())
        {
            this.readOnlyConcurrency.incrementAndGet();
            this.runningReadOnlyTrans.add(context);
        }
        else
        {
            this.runningWriteTrans.add(context);
        }
    }

    public synchronized boolean setTransCommit(long transId)
    {
        TransContext context = this.transIdToContext.get(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.COMMIT);
            return terminateTrans(context);
        }
        return false;
    }

    public synchronized boolean setTransRollback(long transId)
    {
        TransContext context = this.transIdToContext.get(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.ROLLBACK);
            return terminateTrans(context);
        }
        return false;
    }

    private boolean terminateTrans(TransContext context)
    {
        if (context.isReadOnly())
        {
            this.readOnlyConcurrency.decrementAndGet();
            this.runningReadOnlyTrans.remove(context);
        }
        else
        {
            // only clear the context of write transactions
            this.transIdToContext.remove(context.getTransId());
            this.runningWriteTrans.remove(context);
            String traceId = this.transIdToTraceId.remove(context.getTransId());
            if (traceId != null)
            {
                this.traceIdToTransId.remove(traceId);
            }
        }
        return true;
    }

    public synchronized TransContext getTransContext(long transId)
    {
        return this.transIdToContext.get(transId);
    }

    public synchronized long getMinRunningTransTimestamp(boolean readOnly)
    {
        Iterator<TransContext> iterator;
        if (readOnly)
        {
            iterator = this.runningReadOnlyTrans.iterator();
        }
        else
        {
            iterator = this.runningWriteTrans.iterator();
        }
        if (iterator.hasNext())
        {
            return iterator.next().getTimestamp();
        }
        return 0;
    }

    public synchronized boolean isTransExist(long transId)
    {
        return this.transIdToContext.containsKey(transId);
    }

    public synchronized TransContext getTransContext(String externalTraceId)
    {
        Long transId = this.traceIdToTransId.get(externalTraceId);
        if (transId != null)
        {
            return this.transIdToContext.get(transId);
        }
        return null;
    }

    public synchronized boolean bindExternalTraceId(long transId, String externalTraceId)
    {
        if (this.transIdToContext.containsKey(transId))
        {
            this.transIdToTraceId.put(transId, externalTraceId);
            this.traceIdToTransId.put(externalTraceId, transId);
            return true;
        }
        return false;
    }

    /**
     * @param readOnly true to get the concurrency of read-only transactions,
     *                 false to get the concurrency of write transactions.
     * @return the concurrency of the type of transactions.
     */
    public synchronized int getQueryConcurrency(boolean readOnly)
    {
        if (readOnly)
        {
            return this.readOnlyConcurrency.get();
        }
        return this.transIdToContext.size() - this.readOnlyConcurrency.get();
    }
}
