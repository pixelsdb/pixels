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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.daemon.TransProto;
import io.pixelsdb.pixels.daemon.TransServiceGrpc;

import java.util.concurrent.TimeUnit;

/**
 * @create 2022-02-20
 * @update 2023-05-02 merge transaction context management into trans service.
 * @author hank
 */
public class TransService
{
    private final ManagedChannel channel;
    private final TransServiceGrpc.TransServiceBlockingStub stub;

    public TransService(String host, int port)
    {
        assert (host != null);
        assert (port > 0 && port <= 65535);
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
        this.stub = TransServiceGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException
    {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * @param readOnly true if the transaction is determined to be read only, false otherwise.
     * @return the initialized context of the transaction, containing the allocated trans id and timestamp.
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
        TransContextCache.Instance().addTransContext(context);
        MetadataCache.Instance().initCache(context.getTransId());
        return context;
    }

    public boolean commitTrans(long transId, long timestamp) throws TransException
    {
        TransProto.CommitTransRequest request = TransProto.CommitTransRequest.newBuilder()
                .setTransId(transId).setTimestamp(timestamp).build();
        TransProto.CommitTransResponse response = this.stub.commitTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to commit transaction, error code=" + response.getErrorCode());
        }
        TransContextCache.Instance().setTransCommit(transId);
        MetadataCache.Instance().dropCache(transId);
        return true;
    }

    public boolean rollbackTrans(long transId) throws TransException
    {
        TransProto.RollbackTransRequest request = TransProto.RollbackTransRequest.newBuilder()
                .setTransId(transId).build();
        TransProto.RollbackTransResponse response = this.stub.rollbackTrans(request);
        if (response.getErrorCode() != ErrorCode.SUCCESS)
        {
            throw new TransException("failed to rollback transaction, error code=" + response.getErrorCode());
        }
        TransContextCache.Instance().setTransRollback(transId);
        MetadataCache.Instance().dropCache(transId);
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
     * @param newScanBytes the new scan bytes to set in the transaction context
     * @param addCostCents the additional cost in cents to add into the transaction context
     * @return true of the costs are updates successfully, otherwise false
     * @throws TransException if the transaction does not exist
     */
    public boolean updateQueryCosts(long transId, double newScanBytes, QueryCost addCostCents) throws TransException
    {
        TransProto.UpdateQueryCostsRequest request = null;
        if (addCostCents.getType() == QueryCostType.VMCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                .setTransId(transId).setNewScanBytes(newScanBytes).setAddVMCostCents(addCostCents.getCostCents()).build();
        }
        else if (addCostCents.getType() == QueryCostType.CFCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setTransId(transId).setNewScanBytes(newScanBytes).setAddCFCostCents(addCostCents.getCostCents()).build();
        }
        assert(request != null);
        return updateQueryCosts(request);
    }

    /**
     * Update the costs of a transaction (query).
     * @param externalTraceId the external trace id (token) of the transaction
     * @param newScanBytes the new scan bytes to set in the transaction context
     * @param addCostCents the additional cost in cents to add into the transaction context
     * @return true of the costs are updates successfully, otherwise false
     * @throws TransException if the transaction does not exist
     */
    public boolean updateQueryCosts(String externalTraceId, double newScanBytes, QueryCost addCostCents) throws TransException
    {
        TransProto.UpdateQueryCostsRequest request = null;
        if (addCostCents.getType() == QueryCostType.VMCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setExternalTraceId(externalTraceId).setNewScanBytes(newScanBytes).setAddVMCostCents(addCostCents.getCostCents()).build();
        }
        else if (addCostCents.getType() == QueryCostType.CFCOST)
        {
            request = TransProto.UpdateQueryCostsRequest.newBuilder()
                    .setExternalTraceId(externalTraceId).setNewScanBytes(newScanBytes).setAddCFCostCents(addCostCents.getCostCents()).build();
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
}
