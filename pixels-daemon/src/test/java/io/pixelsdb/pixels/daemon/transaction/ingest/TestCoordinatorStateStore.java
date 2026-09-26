/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 */
package io.pixelsdb.pixels.daemon.transaction.ingest;

import io.pixelsdb.pixels.ingest.IngestProto.CoordinatorSnapshot;
import io.pixelsdb.pixels.ingest.IngestProto.DecisionOutcome;
import io.pixelsdb.pixels.ingest.IngestProto.PublicationProgress;
import io.pixelsdb.pixels.ingest.IngestProto.TerminalTransaction;
import io.pixelsdb.pixels.ingest.IngestProto.Transaction;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestCoordinatorStateStore
{
    private static final int MAXIMUM_BYTES = 1024 * 1024;
    private static final int COMPACTION_BYTES = MAXIMUM_BYTES / 2;
    private static final int SNAPSHOT_VERSION = 2;

    @TempDir Path directory;

    @Test
    public void replacementsRetirementAndCheckpointSurviveRestart() throws Exception
    {
        Transaction open = transaction(11L, TransactionState.OPEN, 0L);
        Transaction committed = transaction(11L, TransactionState.COMMIT_DECIDED, 31L);
        CoordinatorSnapshot initial = snapshot(10L, 10L)
                .addTransactions(open)
                .build();
        CoordinatorSnapshot decided = snapshot(10L, 31L)
                .addTransactions(committed)
                .build();
        CoordinatorSnapshot retired = snapshot(31L, 31L)
                .addTerminalTransactions(TerminalTransaction.newBuilder()
                        .setTransaction(committed.toBuilder()
                                .setState(TransactionState.PUBLISHED)
                                .setProgress(PublicationProgress.VISIBLE_NOW))
                        .setRetiredAtMillis(100L))
                .setRetiredTransactionIdHighWatermark(11L)
                .build();

        try (CoordinatorStateStore store = store(directory))
        {
            store.store(initial, DurableIngestCoordinator.StateStore.Durability.DEFERRED);
            store.store(decided, DurableIngestCoordinator.StateStore.Durability.SYNCHRONIZED);
            store.store(retired);
        }
        try (CoordinatorStateStore recovered = store(directory))
        {
            assertEquals(retired, recovered.read());
        }

        Path compacted = directory.resolve("compacted");
        try (CoordinatorStateStore store =
                new CoordinatorStateStore(compacted, MAXIMUM_BYTES, 64))
        {
            store.store(initial);
            store.store(decided);
            assertEquals(decided, store.read());
        }
        try (CoordinatorStateStore recovered =
                new CoordinatorStateStore(compacted, MAXIMUM_BYTES, 64))
        {
            assertEquals(decided, recovered.read());
        }

        Path graceful = directory.resolve("graceful");
        try (CoordinatorStateStore store = store(graceful))
        {
            store.store(initial, DurableIngestCoordinator.StateStore.Durability.DEFERRED);
        }
        try (CoordinatorStateStore recovered = store(graceful))
        {
            assertEquals(initial, recovered.read());
        }
    }

    @Test
    public void tornTailIsDiscardedButConfirmedCorruptionFailsClosed() throws Exception
    {
        CoordinatorSnapshot value = snapshot(10L, 10L).build();
        try (CoordinatorStateStore store = store(directory))
        {
            store.store(value);
        }
        Path journal = directory.resolve("decisions.log");
        try (FileChannel channel = FileChannel.open(journal, StandardOpenOption.APPEND))
        {
            channel.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
            channel.force(true);
        }
        try (CoordinatorStateStore recovered = store(directory))
        {
            assertEquals(value, recovered.read());
        }

        Path corruptDirectory = directory.resolve("corrupt");
        try (CoordinatorStateStore store = store(corruptDirectory))
        {
            store.store(value);
        }
        Path corruptJournal = corruptDirectory.resolve("decisions.log");
        try (FileChannel channel = FileChannel.open(corruptJournal,
                StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            long last = channel.size() - 1L;
            ByteBuffer byteValue = ByteBuffer.allocate(1);
            channel.read(byteValue, last);
            byteValue.flip();
            byteValue.put(0, (byte) (byteValue.get(0) ^ 1));
            channel.write(byteValue, last);
            channel.force(true);
        }
        assertThrows(IOException.class, () -> store(corruptDirectory));
    }

    private static CoordinatorStateStore store(Path path) throws IOException
    {
        return new CoordinatorStateStore(path, MAXIMUM_BYTES, COMPACTION_BYTES);
    }

    private static CoordinatorSnapshot.Builder snapshot(long published, long committed)
    {
        return CoordinatorSnapshot.newBuilder()
                .setVersion(SNAPSHOT_VERSION)
                .setPublishedTimestamp(published)
                .setLastCommitTimestamp(committed);
    }

    private static Transaction transaction(long id, TransactionState state, long commitTimestamp)
    {
        Transaction.Builder transaction = Transaction.newBuilder()
                .setTransactionId(id)
                .setRequestId("request-" + id)
                .setState(state)
                .setOutcome(commitTimestamp == 0L
                        ? DecisionOutcome.UNDECIDED : DecisionOutcome.COMMIT)
                .setProgress(commitTimestamp == 0L
                        ? PublicationProgress.PRIVATE : PublicationProgress.INSTALLING);
        if (commitTimestamp != 0L)
        {
            transaction.setCommitTimestamp(commitTimestamp)
                    .setCommitToken("token-" + id);
        }
        return transaction.build();
    }
}
