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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.TransProto;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.Objects.requireNonNull;

/**
 * This context manager of the transactions.
 *
 * @author hank
 * @update 2023-05-02
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

    private final Map<Long, TransContext> transIdToContext = new ConcurrentHashMap<>();
    /**
     * Two different transactions will not have the same transaction id, so we can store the running transactions using
     * a sorted set that sorts the transaction contexts by transaction timestamp and id. This is important for watermark pushing
     */
    private final Set<TransContext> runningReadOnlyTrans = new ConcurrentSkipListSet<>();
    private final Set<TransContext> runningWriteTrans = new ConcurrentSkipListSet<>();

    private final Map<String, Long> traceIdToTransId = new ConcurrentHashMap<>();
    private final Map<Long, String> transIdToTraceId = new ConcurrentHashMap<>();
    private final AtomicInteger readOnlyConcurrency = new AtomicInteger(0);

    private final ReadWriteLock contextLock = new ReentrantReadWriteLock();

    private TransContextManager() { }

    public void addTransContext(TransContext context)
    {
        requireNonNull(context, "transaction context is null");
        try
        {
            this.contextLock.readLock().lock();
            this.transIdToContext.put(context.getTransId(), context);
            if (context.isReadOnly())
            {
                this.readOnlyConcurrency.incrementAndGet();
                this.runningReadOnlyTrans.add(context);
            } else
            {
                this.runningWriteTrans.add(context);
            }
        }
        finally
        {
            this.contextLock.readLock().unlock();
        }
    }

    public boolean setTransCommit(long transId)
    {
        try
        {
            this.contextLock.readLock().lock();
            TransContext context = this.transIdToContext.get(transId);
            if (context != null)
            {
                context.setStatus(TransProto.TransStatus.COMMIT);
                return terminateTrans(context);
            }
            return false;
        }
        finally
        {
            this.contextLock.readLock().unlock();
        }
    }

    public boolean setTransRollback(long transId)
    {
        try
        {
            this.contextLock.readLock().lock();
            TransContext context = this.transIdToContext.get(transId);
            if (context != null)
            {
                context.setStatus(TransProto.TransStatus.ROLLBACK);
                return terminateTrans(context);
            }
            return false;
        }
        finally
        {
            this.contextLock.readLock().unlock();
        }
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

    public boolean dumpTransContext(long timestamp)
    {
        try
        {
            this.contextLock.writeLock().lock();
            //dump metrics to file
            ConfigFactory config = ConfigFactory.Instance();
            String path = config.getProperty("pixels.historyData.dir") + timestamp + ".csv";
            File file = new File(path);
            boolean newFile = !file.exists();
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(path, true)))
            {
                if (newFile)
                {
                    bw.write("createdTime,memoryUsed,cpuTimeTotal,endTime");
                    bw.newLine();
                }
                for (Long transId : transIdToContext.keySet())
                {
                    TransContext context = getTransContext(transId);
                    if (context.getStatus() == TransProto.TransStatus.COMMIT)
                    {
                        Properties properties = context.getProperties();
                        for (Map.Entry<Object, Object> entry : properties.entrySet())
                        {
                            String e = (String) entry.getKey();
                            if (Objects.equals(e, "queryinfo"))
                            {
                                String v = (String) entry.getValue();
                                bw.write(v);
                                bw.newLine();
                            }
                        }
                    }
                }
                transIdToContext.entrySet().removeIf(entry ->
                        entry.getValue().getStatus() == TransProto.TransStatus.COMMIT);
            } catch (IOException e)
            {
                System.err.println("An error occurred: " + e.getMessage());
            }
            return true;
        }
        finally
        {
            this.contextLock.writeLock().unlock();
        }
    }

    public long getMinRunningTransTimestamp(boolean readOnly)
    {
        try
        {
            this.contextLock.writeLock().lock();
            Iterator<TransContext> iterator;
            if (readOnly)
            {
                iterator = this.runningReadOnlyTrans.iterator();
            } else
            {
                iterator = this.runningWriteTrans.iterator();
            }
            if (iterator.hasNext())
            {
                return iterator.next().getTimestamp();
            }
            return 0;
        }
        finally
        {
            this.contextLock.writeLock().unlock();
        }
    }

    public TransContext getTransContext(long transId)
    {
        return this.transIdToContext.get(transId);
    }

    public boolean isTransExist(long transId)
    {
        return this.transIdToContext.containsKey(transId);
    }

    public TransContext getTransContext(String externalTraceId)
    {
        Long transId = this.traceIdToTransId.get(externalTraceId);
        if (transId != null)
        {
            return this.transIdToContext.get(transId);
        }
        return null;
    }

    public boolean bindExternalTraceId(long transId, String externalTraceId)
    {
        try
        {
            this.contextLock.writeLock().lock();
            if (this.transIdToContext.containsKey(transId))
            {
                this.transIdToTraceId.put(transId, externalTraceId);
                this.traceIdToTransId.put(externalTraceId, transId);
                return true;
            }
            return false;
        }
        finally
        {
            this.contextLock.writeLock().unlock();
        }
    }

    /**
     * @param readOnly true to get the concurrency of read-only transactions,
     *                 false to get the concurrency of write transactions.
     * @return the concurrency of the type of transactions.
     */
    public int getQueryConcurrency(boolean readOnly)
    {
        if (readOnly)
        {
            return this.readOnlyConcurrency.get();
        }
        try
        {
            this.contextLock.writeLock().lock();
            return this.transIdToContext.size() - this.readOnlyConcurrency.get();
        }
        finally
        {
            this.contextLock.writeLock().unlock();
        }
    }
}
