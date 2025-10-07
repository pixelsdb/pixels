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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This context manager of the transactions.
 *
 * @author hank
 * @create 2023-05-02
 * @update 2025-10-05 replace synchronized methods with stamped (optimistic) read-write lock to improve performance
 */
public class TransContextManager
{
    private static final Logger log = LogManager.getLogger(TransContextManager.class);

    private static final class InstanceHolder
    {
        static final TransContextManager instance = new TransContextManager();
    }

    protected static TransContextManager Instance()
    {
        return InstanceHolder.instance;
    }

    private final Map<Long, TransContext> transIdToContext = new ConcurrentHashMap<>();
    /**
     * Two different transactions will not have the same transaction id, so we can store the running transactions using
     * a sorted set that sorts the transaction contexts by transaction timestamp and id. This is important for watermark pushing.
     */
    private final Set<TransContext> runningReadOnlyTrans = new ConcurrentSkipListSet<>();
    private final Set<TransContext> runningWriteTrans = new ConcurrentSkipListSet<>();

    private final Map<String, Long> traceIdToTransId = new ConcurrentHashMap<>();
    private final Map<Long, String> transIdToTraceId = new ConcurrentHashMap<>();
    private final AtomicInteger readOnlyConcurrency = new AtomicInteger(0);

    private final ReadWriteLock contextLock = new ReentrantReadWriteLock();

    private TransContextManager() { }

    /**
     * Add a trans context when a new transaction begins.
     * @param context the trans context
     */
    public void addTransContext(TransContext context)
    {
        requireNonNull(context, "context is null");
        this.contextLock.readLock().lock();
        try
        {
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

    /**
     * Add a batch of trans contexts when new transactions begin.
     * @param contexts the batch of trans contexts
     */
    public void addTransContextBatch(TransContext[] contexts)
    {
        requireNonNull(contexts, "contexts is null");
        this.contextLock.readLock().lock();
        try
        {
            for (TransContext context : contexts)
            {
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
        }
        finally
        {
            this.contextLock.readLock().unlock();
        }
    }

    /**
     * Set the transaction to commit and remove it from this manager.
     * This should be only called when the transaction is about to commit. This method does not block
     * {@link #addTransContext(TransContext)} as the same transaction can only commit after begin.
     * @param transId the trans id
     * @return true on success
     */
    public boolean setTransCommit(long transId)
    {
        this.contextLock.readLock().lock();
        try
        {
            return terminateTrans(transId, TransProto.TransStatus.COMMIT);
        }
        finally
        {
            this.contextLock.readLock().unlock();
        }
    }

    /**
     * Set a batch of transactions to commit and remove them from this manager.
     * This should be only called when the transactions are about to commit. This method does not block
     * {@link #addTransContext(TransContext)} as the same transaction can only commit after begin.
     * @param transIds the trans ids
     * @param success whether each transaction commits successfully
     * @return true if all transactions commit successfully
     */
    public boolean setTransCommitBatch(long[] transIds, Boolean[] success)
    {
        requireNonNull(transIds, "transIds is null");
        requireNonNull(success, "success is null");
        checkArgument(transIds.length == success.length,
                "transIds and success have different length");
        this.contextLock.readLock().lock();
        try
        {
            boolean allSuccess = true;
            for (int i = 0; i < transIds.length; i++)
            {
                success[i] = terminateTrans(transIds[i], TransProto.TransStatus.COMMIT);
                if(!success[i])
                {
                    allSuccess = false;
                    log.error("failed to commit transaction id {}", transIds[i]);
                }
            }
            return allSuccess;
        }
        finally
        {
            this.contextLock.readLock().unlock();
        }
    }

    /**
     * Set the transaction to rollback and remove it from this manager.
     * This should be only called when the transaction is about to roll back. This method does not block
     * {@link #addTransContext(TransContext)} as the same transaction can only roll back after begin.
     * @param transId the trans id
     * @return true on success
     */
    public boolean setTransRollback(long transId)
    {
        this.contextLock.readLock().lock();
        try
        {
            return terminateTrans(transId, TransProto.TransStatus.ROLLBACK);
        }
        finally
        {
            this.contextLock.readLock().unlock();
        }
    }

    private boolean terminateTrans(long transId, TransProto.TransStatus status)
    {
        TransContext context = this.transIdToContext.get(transId);
        if (context != null)
        {
            context.setStatus(status);
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
        return false;
    }

    /**
     * Dump the context of transactions in this manager to a history file and remove terminated transactions. This method
     * blocks {@link #addTransContext(TransContext)}, {@link #setTransCommit(long)}, {@link #setTransRollback(long)},
     * {@link #bindExternalTraceId(long, String)}, and {@link #getQueryConcurrency(boolean)}. Hence, it should be used carefully.
     * @param timestamp the snapshot timestamp
     * @return true on success
     */
    public boolean dumpTransContext(long timestamp)
    {
        this.contextLock.writeLock();
        try
        {
            //dump metrics to file
            ConfigFactory config = ConfigFactory.Instance();
            String path = config.getProperty("pixels.history.data.dir") + timestamp + ".csv";
            File file = new File(path);
            boolean newFile = !file.exists();
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(path, true)))
            {
                if (newFile)
                {
                    bw.write("createdTime,memoryUsed,cpuTimeTotal,endTime");
                    bw.newLine();
                }
                for (TransContext context : this.transIdToContext.values())
                {
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
                        entry.getValue().getStatus() == TransProto.TransStatus.COMMIT ||
                                entry.getValue().getStatus() == TransProto.TransStatus.ROLLBACK);
            } catch (IOException e)
            {
                log.error("failed to dump transaction contexts", e);
                return false;
            }
            return true;
        }
        finally
        {
            this.contextLock.writeLock().unlock();
        }
    }

    /**
     * Get the minimal timestamp of running readonly or non-readonly transactions.
     * This method does not acquire a lock as it only reads a snapshot of the current running transactions.
     * @param readOnly readonly trans or not
     * @return the minimal transaction time stamp
     */
    public long getMinRunningTransTimestamp(boolean readOnly)
    {
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
        // Issue #1099: no lock needed as it does not modify anything.
        Long transId = this.traceIdToTransId.get(externalTraceId);
        if (transId != null)
        {
            return this.transIdToContext.get(transId);
        }
        return null;
    }

    public boolean bindExternalTraceId(long transId, String externalTraceId)
    {
        this.contextLock.writeLock();
        try
        {
            // Issue #1099: write lock needed to block concurrent transaction commit/rollback.
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
        // Issue #1099: write lock is needed to block concurrent transaction begin/commit/rollback.
        this.contextLock.writeLock();
        try
        {
            return this.transIdToContext.size() - this.readOnlyConcurrency.get();
        }
        finally
        {
            this.contextLock.writeLock().unlock();
        }
    }
}
