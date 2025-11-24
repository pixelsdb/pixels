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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.server.HostAddress;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.ShutdownHookManager;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author hank
 * @create 2022-02-20
 * @update 2023-05-02 merge transaction context management into trans service.
 * @update 2025-10-05 eliminate transaction context cache and metadata cache for non-readonly transactions.
 */
public class TransService
{
    private static final Logger logger = LogManager.getLogger(TransService.class);
    private static final TransService defaultInstance;
    private static final Map<HostAddress, TransService> otherInstances = new ConcurrentHashMap<>();

    static
    {
        String transHost = ConfigFactory.Instance().getProperty("trans.server.host");
        int transPort = Integer.parseInt(ConfigFactory.Instance().getProperty("trans.server.port"));
        defaultInstance = new TransService(transHost, transPort);
        ShutdownHookManager.Instance().registerShutdownHook(TransService.class, false, () -> {
            try
            {
                defaultInstance.shutdown();
                for (TransService otherTransService : otherInstances.values())
                {
                    otherTransService.shutdown();
                }
                otherInstances.clear();
            } catch (InterruptedException e)
            {
                logger.error("failed to shut down trans service", e);
            }
        });
    }

    /**
     * Get the default trans service instance connecting to the trans host:port configured in
     * PIXELS_HOME/etc/pixels.properties. This default instance will be automatically shut down when the process
     * is terminating, no need to call {@link #shutdown()} (although it is idempotent) manually.
     * @return the default trans service instance
     */
    public static TransService Instance()
    {
        return defaultInstance;
    }

    /**
     * This method should only be used to connect to a trans server that is not configured through
     * PIXELS_HOME/etc/pixels.properties. <b>No need</b> to manually shut down the returned trans service.
     * @param host the host name of the trans server
     * @param port the port of the trans server
     * @return the created trans service instance
     */
    public static synchronized TransService CreateInstance(String host, int port)
    {
        HostAddress address = HostAddress.fromParts(host, port);
        TransService transService = otherInstances.get(address);
        if (transService != null)
        {
            return transService;
        }
        transService = new TransService(host, port);
        otherInstances.put(address, transService);
        return transService;
    }

    private final ManagedChannel channel;
    private final TransServiceGrpc.TransServiceBlockingStub stub;
    private boolean isShutDown;

    private TransService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = TransServiceGrpc.newBlockingStub(channel);
        this.isShutDown = false;
    }

    private synchronized void shutdown() throws InterruptedException
    {
        if (!this.isShutDown)
        {
            this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            this.isShutDown = true;
        }
    }

    /**
     * Begin a transaction.
     * @param readOnly true if the transaction is determined to be read only, false otherwise
     * @return the initialized context of the transaction, containing the allocated trans id and timestamp
     * @throws TransException
     */
    public TransContext beginTrans(boolean readOnly) throws TransException
    {
        TransProto.BeginTransRequest request = TransProto.BeginTransRequest.newBuilder()
                .setReadOnly(readOnly).build();
        TransProto.BeginTransResponse response = this.stub.beginTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to begin transaction, error code=" + response.getErrorCode());
        }
        TransContext context = new TransContext(response.getTransId(), response.getTimestamp(), readOnly);
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            TransContextCache.Instance().addTransContext(context);
            MetadataCache.Instance().initCache(context.getTransId());
        }
        return context;
    }

    /**
     * Begin a batch of transactions.
     * @param numTrans the number of transaction to begin as a batch
     * @param readOnly true if the transaction is determined to be read only, false otherwise
     * @return the initialized contexts of the transactions in the batch
     * @throws TransException
     */
    public List<TransContext> beginTransBatch(int numTrans, boolean readOnly) throws TransException
    {
        TransProto.BeginTransBatchRequest request = TransProto.BeginTransBatchRequest.newBuilder()
                .setReadOnly(readOnly).setExpectNumTrans(numTrans).build();
        TransProto.BeginTransBatchResponse response = this.stub.beginTransBatch(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to begin the batch of transactions, error code=" + response.getErrorCode());
        }
        ImmutableList.Builder<TransContext> contexts = ImmutableList.builder();
        for (int i = 0; i < response.getExactNumTrans(); i++)
        {
            long transId = response.getTransIds(i);
            long timestamp = response.getTimestamps(i);
            TransContext context = new TransContext(transId, timestamp, readOnly);

            if (readOnly)
            {
                // Issue #1099: only use trans context cache and metadata cache for read only queries.
                TransContextCache.Instance().addTransContext(context);
                MetadataCache.Instance().initCache(context.getTransId());
            }
            contexts.add(context);
        }
        return contexts.build();
    }

    /**
     * Commit a transaction.
     * @param transId the transaction id
     * @param readOnly true if the transaction is readonly
     * @return true on success
     * @throws TransException
     */
    public boolean commitTrans(long transId , boolean readOnly) throws TransException
    {
        TransProto.CommitTransRequest request = TransProto.CommitTransRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.CommitTransResponse response = this.stub.commitTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to commit transaction, error code=" + response.getErrorCode());
        }
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            TransContextCache.Instance().setTransCommit(transId);
            MetadataCache.Instance().dropCache(transId);
        }
        return true;
    }

    /**
     * Commit a batch of transactions and return whether the execution succeeded.
     * If execution fails, specific error logs can be obtained from the transService logs,
     * such as the transaction does not exist or the commit fails.
     * @param transIds transaction ids of the transactions to commit
     * @param readOnly true if the transactions are readonly
     * @return whether each transaction was successfully committed
     * @throws TransException
     */
    public List<Boolean> commitTransBatch(List<Long> transIds, boolean readOnly) throws TransException
    {
        if (transIds == null || transIds.isEmpty())
        {
            throw new IllegalArgumentException("transIds is null or empty");
        }
        TransProto.CommitTransBatchRequest request = TransProto.CommitTransBatchRequest.newBuilder()
                .addAllTransIds(transIds).build();
        TransProto.CommitTransBatchResponse response = this.stub.commitTransBatch(request);
        if (response.getErrorCode() == ErrorCode.TRANS_INVALID_ARGUMENT) // other error codes are not thrown as exceptions
        {
            throw new TransException("transaction ids and timestamps size mismatch");
        }
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            for (long transId : transIds)
            {
                TransContextCache.Instance().setTransCommit(transId);
                MetadataCache.Instance().dropCache(transId);
            }
        }
        return response.getResultsList();
    }

    /**
     * Rollback a transaction.
     * @param transId the transaction id
     * @param readOnly true if the transaction is readonly
     * @return true on success
     * @throws TransException
     */
    public boolean rollbackTrans(long transId, boolean readOnly) throws TransException
    {
        TransProto.RollbackTransRequest request = TransProto.RollbackTransRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.RollbackTransResponse response = this.stub.rollbackTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to rollback transaction, error code=" + response.getErrorCode());
        }
        if (readOnly)
        {
            // Issue #1099: only use trans context cache and metadata cache for read only queries.
            TransContextCache.Instance().setTransRollback(transId);
            MetadataCache.Instance().dropCache(transId);
        }
        return true;
    }

    public TransContext getTransContext(long transId) throws TransException
    {
        TransProto.GetTransContextRequest request = TransProto.GetTransContextRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.GetTransContextResponse response = this.stub.getTransContext(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get transaction context, error code=" + response.getErrorCode());
        }
        return new TransContext(response.getTransContext());
    }

    public TransContext getTransContext(String externalTraceId) throws TransException
    {
        TransProto.GetTransContextRequest request = TransProto.GetTransContextRequest.newBuilder()
                .setExternalTraceId(externalTraceId).build();
        TransProto.GetTransContextResponse response = this.stub.getTransContext(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get transaction context, error code=" + response.getErrorCode());
        }
        return new TransContext(response.getTransContext());
    }

    /**
     * Set the string property of a transaction.
     * @param transId the id of the transaction
     * @param key the property key
     * @param value the property value
     * @return the previous value of the property key, or null if not present
     * @throws TransException if the transaction does not exist
     */
    public String setTransProperty(long transId, String key, String value) throws TransException
    {
        TransProto.SetTransPropertyRequest request = TransProto.SetTransPropertyRequest.newBuilder()
                .setTransId(transId).setKey(key).setValue(value).build();
        return setTransProperty(request);
    }

    /**
     * Set the string property of a transaction.
     * @param externalTraceId the external trace id (token) of the transaction
     * @param key the property key
     * @param value the property value
     * @return the previous value of the property key, or null if not present
     * @throws TransException if the transaction does not exist
     */
    public String setTransProperty(String externalTraceId, String key, String value) throws TransException
    {
        TransProto.SetTransPropertyRequest request = TransProto.SetTransPropertyRequest.newBuilder()
                .setExternalTraceId(externalTraceId).setKey(key).setValue(value).build();
        return setTransProperty(request);
    }

    private String setTransProperty(TransProto.SetTransPropertyRequest request) throws TransException
    {
        TransProto.SetTransPropertyResponse response = this.stub.setTransProperty(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to set transaction property, error code=" + response.getErrorCode());
        }
        if (response.hasPrevValue())
        {
            return response.getPrevValue();
        }
        return null;
    }

    /**
     * Update the costs of a transaction (query).
     * @param transId the id of the transaction
     * @param scanBytes the scan bytes to set in the transaction context
     * @param costCents the cost in cents to set in the transaction context
     * @return true of the costs are updates successfully, otherwise false
     * @throws TransException if the transaction does not exist
     */
    public boolean updateQueryCosts(long transId, double scanBytes, QueryCost costCents) throws TransException
    {
        TransProto.UpdateQueryCostsRequest request = null;
        if (costCents.getType() == QueryCostType.VMCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                .setTransId(transId).setScanBytes(scanBytes).setVmCostCents(costCents.getCostCents()).build();
        }
        else if (costCents.getType() == QueryCostType.CFCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setTransId(transId).setScanBytes(scanBytes).setCfCostCents(costCents.getCostCents()).build();
        }
        assert(request != null);
        return updateQueryCosts(request);
    }

    /**
     * Update the costs of a transaction (query).
     * @param externalTraceId the external trace id (token) of the transaction
     * @param scanBytes the scan bytes to set in the transaction context
     * @param costCents the cost in cents to set in the transaction context
     * @return true of the costs are updates successfully, otherwise false
     * @throws TransException if the transaction does not exist
     */
    public boolean updateQueryCosts(String externalTraceId, double scanBytes, QueryCost costCents) throws TransException
    {
        TransProto.UpdateQueryCostsRequest request = null;
        if (costCents.getType() == QueryCostType.VMCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setExternalTraceId(externalTraceId).setScanBytes(scanBytes).setVmCostCents(costCents.getCostCents()).build();
        }
        else if (costCents.getType() == QueryCostType.CFCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setExternalTraceId(externalTraceId).setScanBytes(scanBytes).setCfCostCents(costCents.getCostCents()).build();
        }
        assert(request != null);
        return updateQueryCosts(request);
    }

    private boolean updateQueryCosts(TransProto.UpdateQueryCostsRequest request) throws TransException
    {
        TransProto.UpdateQueryCostsResponse response = this.stub.updateQueryCosts(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to update query costs, error code=" + response.getErrorCode());
        }
        return true;
    }

    public int getTransConcurrency(boolean readOnly) throws TransException
    {
        TransProto.GetTransConcurrencyRequest request = TransProto.GetTransConcurrencyRequest.newBuilder()
                .setReadOnly(readOnly).build();
        TransProto.GetTransConcurrencyResponse response = this.stub.getTransConcurrency(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get transaction concurrency, error code=" + response.getErrorCode());
        }
        return response.getConcurrency();
    }

    public boolean bindExternalTraceId(long transId, String externalTraceId) throws TransException
    {
        TransProto.BindExternalTraceIdRequest request = TransProto.BindExternalTraceIdRequest.newBuilder()
                .setTransId(transId).setExternalTraceId(externalTraceId).build();
        TransProto.BindExternalTraceIdResponse response = this.stub.bindExternalTraceId(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to bind transaction id and external trace id, error code="
                    + response.getErrorCode());
        }
        return true;
    }

    public long getSafeGcTimestamp() throws TransException
    {
        TransProto.GetSafeGcTimestampResponse response = this.stub.getSafeGcTimestamp(Empty.getDefaultInstance());
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to get safe garbage collection timestamp"
                    + response.getErrorCode());
        }
        return response.getTimestamp();
    }
}
