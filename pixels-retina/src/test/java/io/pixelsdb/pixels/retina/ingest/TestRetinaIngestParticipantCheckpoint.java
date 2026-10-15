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
import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.PrepareToken;
import io.pixelsdb.pixels.ingest.IngestProto.DecisionOutcome;
import io.pixelsdb.pixels.ingest.IngestProto.PublicationProgress;
import io.pixelsdb.pixels.ingest.IngestProto.ReadPin;
import io.pixelsdb.pixels.ingest.IngestProto.Route;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import io.pixelsdb.pixels.ingest.IngestProto.Transaction;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionList;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRetinaIngestParticipantCheckpoint
{
    private static final long STATEMENT_ID = 1L;
    private static final long READ_TRANSACTION_ID = 99L;
    private static final long READ_PIN_LEASE_MILLIS = 10_000L;
    private static final long CONCURRENCY_TIMEOUT_SECONDS = 5L;
    private static final int CHECKPOINT_TEST_THREADS = 2;

    @Test
    public void testCheckpointThenWalReclaimSurvivesRestartWithoutReplay() throws Exception
    {
        Path directory = Files.createTempDirectory("pixels-participant-checkpoint-");
        String owner = "127.0.0.1:18889";
        MutationStreamId stream = new MutationStreamId(
                41, STATEMENT_ID, 7, 13, 0, MutationStreamId.Kind.APPEND_ROWS);
        MutationBatch batch = new MutationBatch(stream, 0, 3, 1, 2, new byte[] {4, 1});
        MutationStreamSeal seal = new MutationStreamSeal(
                stream, 1, 2, batch.getPayloadBytes(),
                MutationStreamSeal.extendDigest(
                        MutationStreamSeal.emptyDigest(), batch.getDigest()));
        TableSpec table = TableSpec.newBuilder()
                .setTableId(13)
                .setSchemaVersion(3)
                .setFingerprint(ByteString.copyFromUtf8("table-13-v3"))
                .addRoutes(Route.newBuilder().setShardId(0).setHost("127.0.0.1").setPort(18889))
                .build();
        Transaction unsigned = Transaction.newBuilder()
                .setTransactionId(41)
                .setCommitTimestamp(52)
                .setState(TransactionState.PUBLISHED)
                .setTable(table)
                .addEnlistedTables(table)
                .setOutcome(DecisionOutcome.COMMIT)
                .setProgress(PublicationProgress.VISIBLE_NOW)
                .setCommitToken("checkpoint-test-token")
                .addStreams(IngestWire.encode(stream))
                .addSeals(IngestWire.encode(seal))
                .build();
        Transaction transaction = unsigned.toBuilder()
                .addTokens(PrepareToken.newBuilder()
                        .setOwner(owner)
                        .setDigest(ByteString.copyFrom(IngestWire.prepareDigest(unsigned, owner))))
                .build();
        Decisions decisions = new Decisions(transaction);
        AtomicBoolean durableCheckpoint = new AtomicBoolean();
        AtomicInteger firstInstalls = new AtomicInteger();
        CountDownLatch checkpointStarted = new CountDownLatch(1);
        CountDownLatch releaseCheckpoint = new CountDownLatch(1);

        try
        {
            try (LocalMutationJournal journal = open(directory))
            {
                journal.append(batch);
                journal.seal(seal);
                RetinaIngestParticipant participant = new RetinaIngestParticipant(
                        owner, journal, decisions,
                        new Installer(
                                durableCheckpoint,
                                firstInstalls,
                                checkpointStarted,
                                releaseCheckpoint),
                        new IngestReadPins(READ_PIN_LEASE_MILLIS));
                participant.recover();
                assertEquals(1, firstInstalls.get());
                ExecutorService executor = Executors.newFixedThreadPool(CHECKPOINT_TEST_THREADS);
                try
                {
                    Future<?> checkpoint = executor.submit(() ->
                    {
                        participant.checkpointPublishedTransactions();
                        return null;
                    });
                    assertTrue(checkpointStarted.await(
                            CONCURRENCY_TIMEOUT_SECONDS, TimeUnit.SECONDS));
                    Future<ReadPin> read = executor.submit(() -> participant.pinRead(
                            ReadPin.newBuilder()
                                    .setTransactionId(READ_TRANSACTION_ID)
                                    .setReadTimestamp(transaction.getCommitTimestamp())
                                    .build()));
                    ReadPin pin = read.get(
                            CONCURRENCY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    participant.readPins().release(pin);
                    releaseCheckpoint.countDown();
                    checkpoint.get(CONCURRENCY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                finally
                {
                    releaseCheckpoint.countDown();
                    executor.shutdownNow();
                }
                assertTrue(durableCheckpoint.get());
                expectIo(() -> journal.readSealedBatch(stream, 0));
                expectIo(() -> journal.append(batch));
                participant.close();
            }

            AtomicInteger restartInstalls = new AtomicInteger();
            try (LocalMutationJournal journal = open(directory))
            {
                RetinaIngestParticipant restarted = new RetinaIngestParticipant(
                        owner, journal, decisions,
                        new Installer(durableCheckpoint, restartInstalls),
                        new IngestReadPins(READ_PIN_LEASE_MILLIS));
                restarted.recover();
                assertEquals("checkpointed transaction was replayed", 0, restartInstalls.get());
                expectIo(() -> journal.readSealedBatch(stream, 0));
                expectIo(() -> journal.append(batch));
                restarted.close();
            }
        }
        finally
        {
            try (java.util.stream.Stream<Path> paths = Files.walk(directory))
            {
                Path[] ordered = paths.sorted(java.util.Comparator.reverseOrder()).toArray(Path[]::new);
                for (Path path : ordered)
                {
                    Files.deleteIfExists(path);
                }
            }
        }
    }

    private static LocalMutationJournal open(Path directory) throws IOException
    {
        return new LocalMutationJournal(directory, 1024, 1_000_000, 1000);
    }

    private static void expectIo(Checked action) throws Exception
    {
        try
        {
            action.run();
            fail("expected IOException");
        }
        catch (IOException expected)
        {
            // Expected: the compacted generation retains only its durable late-request fence.
        }
    }

    private interface Checked
    {
        void run() throws Exception;
    }

    private static final class Decisions implements RetinaIngestParticipant.Decisions
    {
        private final Transaction transaction;

        private Decisions(Transaction transaction)
        {
            this.transaction = transaction;
        }

        @Override
        public Transaction get(long id)
        {
            return transaction;
        }

        @Override
        public Transaction abort(long id)
        {
            throw new AssertionError("published transaction must not abort");
        }

        @Override
        public TransactionList list(String owner)
        {
            return TransactionList.newBuilder()
                    .setPublishedTimestamp(transaction.getCommitTimestamp())
                    .addTransactions(transaction)
                    .build();
        }

        public long publishedTimestamp()
        {
            return transaction.getCommitTimestamp();
        }
    }

    private static final class Installer implements RetinaIngestParticipant.Installer
    {
        private final AtomicBoolean checkpoint;
        private final AtomicInteger installs;
        private final CountDownLatch checkpointStarted;
        private final CountDownLatch releaseCheckpoint;

        private Installer(AtomicBoolean checkpoint, AtomicInteger installs)
        {
            this(checkpoint, installs, null, null);
        }

        private Installer(
                AtomicBoolean checkpoint,
                AtomicInteger installs,
                CountDownLatch checkpointStarted,
                CountDownLatch releaseCheckpoint)
        {
            this.checkpoint = checkpoint;
            this.installs = installs;
            this.checkpointStarted = checkpointStarted;
            this.releaseCheckpoint = releaseCheckpoint;
        }

        @Override
        public void prepare(Transaction tx, Iterable<MutationBatch> batches) {}

        @Override
        public boolean install(
                Transaction tx,
                Iterable<MutationBatch> batches,
                boolean recovering,
                boolean forceFileTail)
        {
            for (MutationBatch ignored : batches)
            {
                // Force the recovery path to consume the WAL before it is checkpointed.
            }
            installs.incrementAndGet();
            return true;
        }

        @Override
        public void release(long txId) {}

        @Override
        public void initializeRecovery(java.util.List<Transaction> transactions) {}

        @Override
        public boolean recoveredByCheckpoint(Transaction tx)
        {
            return checkpoint.get();
        }

        @Override
        public boolean checkpoint(Transaction tx, Iterable<MutationBatch> batches)
                throws Exception
        {
            if (checkpointStarted != null)
            {
                checkpointStarted.countDown();
                if (!releaseCheckpoint.await(
                        CONCURRENCY_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                {
                    throw new IOException("Timed out waiting to release checkpoint");
                }
            }
            int count = 0;
            for (MutationBatch ignored : batches)
            {
                count++;
            }
            assertEquals(1, count);
            checkpoint.set(true);
            return true;
        }

        @Override
        public void close() {}
    }
}
