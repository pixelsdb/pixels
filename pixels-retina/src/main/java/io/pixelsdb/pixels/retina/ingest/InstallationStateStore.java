/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.retina.ingest;

import io.pixelsdb.pixels.common.ingest.durable.AtomicStateFile;
import io.pixelsdb.pixels.ingest.IngestProto.BatchInstall;
import io.pixelsdb.pixels.ingest.IngestProto.InstallationSnapshot;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionCheckpoint;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/** Durable installation checkpoints plus an append-only per-batch plan journal. */
public final class InstallationStateStore implements Closeable
{
    static final int SNAPSHOT_VERSION = 4;

    private final AtomicStateFile checkpointState;
    private final InstallationPlanJournal planJournal;

    public InstallationStateStore(
            Path directory, int maximumBytes, int compactionBytes) throws IOException
    {
        this.checkpointState = new AtomicStateFile(directory, maximumBytes);
        try
        {
            this.planJournal = new InstallationPlanJournal(
                    directory, maximumBytes, compactionBytes);
        }
        catch (IOException e)
        {
            checkpointState.close();
            throw e;
        }
    }

    public synchronized Map<String, BatchInstall> plans() throws IOException
    {
        Map<String, BatchInstall> result = new LinkedHashMap<>(planJournal.plans());
        for (long transactionId : checkpoints().keySet())
        {
            result.values().removeIf(plan ->
                    plan.getStream().getTransactionId() == transactionId);
        }
        return result;
    }

    public synchronized Map<Long, TransactionCheckpoint> checkpoints() throws IOException
    {
        Map<Long, TransactionCheckpoint> result = new LinkedHashMap<>();
        byte[] bytes = checkpointState.read();
        if (bytes.length == 0)
        {
            return result;
        }
        InstallationSnapshot snapshot = InstallationSnapshot.parseFrom(bytes);
        if (snapshot.getVersion() != SNAPSHOT_VERSION || snapshot.getBatchesCount() != 0)
        {
            throw new IOException("Unknown installation checkpoint version");
        }
        for (TransactionCheckpoint checkpoint : snapshot.getCheckpointsList())
        {
            if (result.put(checkpoint.getTransactionId(), checkpoint) != null)
            {
                throw new IOException("Duplicate installation checkpoint");
            }
        }
        return result;
    }

    public synchronized void put(BatchInstall plan) throws IOException
    {
        planJournal.put(plan);
    }

    public synchronized void checkpoint(
            long transactionId,
            Collection<TransactionCheckpoint> checkpoints) throws IOException
    {
        storeCheckpoints(checkpoints);
        planJournal.removeTransaction(transactionId);
    }

    public synchronized void storeCheckpoints(
            Collection<TransactionCheckpoint> checkpoints) throws IOException
    {
        checkpointState.store(InstallationSnapshot.newBuilder()
                .setVersion(SNAPSHOT_VERSION)
                .addAllCheckpoints(checkpoints)
                .build()
                .toByteArray());
    }

    public synchronized long size()
    {
        return planJournal.size();
    }

    @Override
    public synchronized void close() throws IOException
    {
        IOException failure = null;
        try
        {
            planJournal.close();
        }
        catch (IOException e)
        {
            failure = e;
        }
        try
        {
            checkpointState.close();
        }
        catch (IOException e)
        {
            if (failure == null)
            {
                failure = e;
            }
            else
            {
                failure.addSuppressed(e);
            }
        }
        if (failure != null)
        {
            throw failure;
        }
    }
}
