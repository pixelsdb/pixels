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

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.daemon.TransProto;

import java.util.HashMap;
import java.util.Map;

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
    private final Map<String, Long> traceIdToTransId = new HashMap<>();
    private final Map<Long, String> transIdToTraceId = new HashMap<>();
    private volatile int readOnlyConcurrency;

    private TransContextManager() { }

    public synchronized void addTrans(TransContext context)
    {
        requireNonNull(context, "transaction context is null");
        this.transIdToContext.put(context.getTransId(), context);
        if (context.isReadOnly())
        {
            this.readOnlyConcurrency++;
        }
    }

    public synchronized void setTransCommit(long transId)
    {
        TransContext context = this.transIdToContext.remove(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.COMMIT);
            if (context.isReadOnly())
            {
                this.readOnlyConcurrency--;
            }
            String traceId = this.transIdToTraceId.remove(transId);
            if (traceId != null)
            {
                this.traceIdToTransId.remove(traceId);
            }
        }
    }

    public synchronized void setTransRollback(long transId)
    {
        TransContext context = this.transIdToContext.remove(transId);
        if (context != null)
        {
            context.setStatus(TransProto.TransStatus.ROLLBACK);
            if (context.isReadOnly())
            {
                this.readOnlyConcurrency--;
            }
            String traceId = this.transIdToTraceId.remove(transId);
            if (traceId != null)
            {
                this.traceIdToTransId.remove(traceId);
            }
        }
    }

    public synchronized TransContext getTransContext(long transId)
    {
        return this.transIdToContext.get(transId);
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

    public synchronized void bindExternalTraceId(long transId, String externalTraceId) throws TransException
    {
        if (this.transIdToContext.containsKey(transId))
        {
            this.transIdToTraceId.put(transId, externalTraceId);
            this.traceIdToTransId.put(externalTraceId, transId);
        }
        throw new TransException(String.format("transaction of id %d does not exist", transId));
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
            return this.readOnlyConcurrency;
        }
        return this.transIdToContext.size() - this.readOnlyConcurrency;
    }
}
