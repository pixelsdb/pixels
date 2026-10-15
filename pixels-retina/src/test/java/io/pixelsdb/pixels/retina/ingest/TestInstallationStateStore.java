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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.ingest.IngestProto.BatchInstall;
import io.pixelsdb.pixels.ingest.IngestProto.BufferSpan;
import io.pixelsdb.pixels.ingest.IngestProto.MutationKind;
import io.pixelsdb.pixels.ingest.IngestProto.StreamId;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionCheckpoint;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInstallationStateStore
{
    private static final int MAXIMUM_BYTES = 1024 * 1024;
    private static final int COMPACTION_BYTES = MAXIMUM_BYTES / 2;
    private static final long TRANSACTION_ID = 17L;

    @Test
    public void latestPlanAndCheckpointSurviveRestart() throws Exception
    {
        Path directory = Files.createTempDirectory("pixels-installation-state-");
        BatchInstall initial = plan(0);
        BatchInstall placed = plan(10);
        try (InstallationStateStore store = store(directory))
        {
            store.put(initial);
            store.put(placed);
        }
        try (InstallationStateStore recovered = store(directory))
        {
            assertEquals(placed, recovered.plans().values().iterator().next());
            TransactionCheckpoint checkpoint = TransactionCheckpoint.newBuilder()
                    .setTransactionId(TRANSACTION_ID)
                    .setCommitTimestamp(23L)
                    .build();
            recovered.checkpoint(TRANSACTION_ID, Collections.singletonList(checkpoint));
        }
        try (InstallationStateStore checkpointed = store(directory))
        {
            assertTrue(checkpointed.plans().isEmpty());
            assertTrue(checkpointed.checkpoints().containsKey(TRANSACTION_ID));
        }
    }

    @Test
    public void tornTailIsDiscardedButConfirmedCorruptionFailsClosed() throws Exception
    {
        Path tornDirectory = Files.createTempDirectory("pixels-installation-torn-");
        BatchInstall plan = plan(10);
        try (InstallationStateStore store = store(tornDirectory))
        {
            store.put(plan);
        }
        Path tornLog = tornDirectory.resolve("plans.log");
        try (FileChannel channel = FileChannel.open(tornLog, StandardOpenOption.APPEND))
        {
            channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
            channel.force(true);
        }
        try (InstallationStateStore recovered = store(tornDirectory))
        {
            assertEquals(plan, recovered.plans().values().iterator().next());
        }

        Path corruptDirectory = Files.createTempDirectory("pixels-installation-corrupt-");
        try (InstallationStateStore store = store(corruptDirectory))
        {
            store.put(plan);
        }
        Path corruptLog = corruptDirectory.resolve("plans.log");
        try (FileChannel channel = FileChannel.open(
                corruptLog, StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            long last = channel.size() - 1L;
            ByteBuffer value = ByteBuffer.allocate(1);
            channel.read(value, last);
            value.flip();
            value.put(0, (byte) (value.get(0) ^ 1));
            channel.write(value, last);
            channel.force(true);
        }
        assertThrows(IOException.class, () -> store(corruptDirectory));
    }

    private static InstallationStateStore store(Path directory) throws IOException
    {
        return new InstallationStateStore(directory, MAXIMUM_BYTES, COMPACTION_BYTES);
    }

    private static BatchInstall plan(int rows)
    {
        BatchInstall.Builder plan = BatchInstall.newBuilder()
                .setStream(StreamId.newBuilder()
                        .setTransactionId(TRANSACTION_ID)
                        .setStatementId(19L)
                        .setWriterId(29L)
                        .setTableId(31L)
                        .setShardId(0)
                        .setKind(MutationKind.APPEND_ROWS))
                .setSequence(0L)
                .setCommitTimestamp(23L)
                .setRowIdStart(100L)
                .setRowCount(10)
                .setDigest(ByteString.copyFromUtf8("digest"));
        if (rows > 0)
        {
            plan.addSpans(BufferSpan.newBuilder()
                    .setFileId(37L)
                    .setFileName("file.pxl")
                    .setPathId(41L)
                    .setFileCapacity(100)
                    .setBlockStartOffset(0)
                    .setRowCount(rows)
                    .setRowIdStart(100L));
        }
        return plan.build();
    }
}
