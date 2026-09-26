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
package io.pixelsdb.pixels.retina.ingest;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.wire.ColumnBatchCodec;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRetinaPrivateRead
{
    private static final long TRANSACTION_ID = 101L;
    private static final long TABLE_ID = 73L;
    private static final long SCHEMA_VERSION = 1L;
    private static final long LAYOUT_ID = 1L;
    private static final long FIRST_STATEMENT_ID = 11L;
    private static final long SECOND_STATEMENT_ID = 12L;
    private static final long READER_STATEMENT_ID = 13L;
    private static final long FIRST_STATEMENT_ORDINAL = 1L;
    private static final long SECOND_STATEMENT_ORDINAL = 2L;
    private static final long READER_STATEMENT_ORDINAL = 3L;
    private static final long PRIVATE_READ_FRONTIER = SECOND_STATEMENT_ORDINAL;
    private static final long FIRST_WRITER_ID = 21L;
    private static final long SECOND_WRITER_ID = 22L;
    private static final long READ_TIMESTAMP = 0L;
    private static final long FIRST_SEQUENCE = 0L;
    private static final long SECOND_SEQUENCE = 1L;
    private static final int SHARD_ID = 0;
    private static final int PARTICIPANT_PORT = 10_000;
    private static final int ROWS_PER_BATCH = 1;
    private static final int PAGE_BATCH_LIMIT = 2;
    private static final int CONFIGURED_BATCH_LIMIT = 4;
    private static final int CONFIGURED_BYTE_LIMIT = 4 * 1024;
    private static final int JOURNAL_PAYLOAD_LIMIT = 1024;
    private static final long JOURNAL_BYTE_LIMIT = 1024L * 1024L;
    private static final int JOURNAL_RECORD_LIMIT = 100;
    private static final int GROUP_COMMIT_STREAMS = 8;
    private static final long GROUP_COMMIT_DELAY_MICROS = 50_000L;
    private static final long GROUP_COMMIT_TIMEOUT_SECONDS = 10L;
    private static final long READ_LEASE_MILLIS = 60_000L;
    private static final String OWNER = "127.0.0.1:" + PARTICIPANT_PORT;
    private static final byte[] FIRST_PAYLOAD = new byte[] {1};
    private static final byte[] SECOND_PAYLOAD = new byte[] {2};
    private static final byte[] THIRD_PAYLOAD = new byte[] {3};
    private static final TableSpec TABLE = TableSpec.newBuilder()
            .setTableId(TABLE_ID)
            .setSchemaName("s")
            .setTableName("t")
            .setSchemaVersion(SCHEMA_VERSION)
            .setLayoutId(LAYOUT_ID)
            .addRoutes(Route.newBuilder().setShardId(SHARD_ID)
                    .setHost("127.0.0.1").setPort(PARTICIPANT_PORT))
            .addColumns(TableColumn.newBuilder().setId(SCHEMA_VERSION)
                    .setName("v").setType("bigint"))
            .build();

    @TempDir Path directory;

    @Test
    public void concurrentParticipantSealsShareWalSync() throws Exception
    {
        MutableDecisions decisions = new MutableDecisions();
        Transaction.Builder transaction = Transaction.newBuilder()
                .setTransactionId(TRANSACTION_ID)
                .setState(TransactionState.OPEN)
                .setTable(TABLE)
                .addEnlistedTables(TABLE);
        List<MutationBatch> batches = new ArrayList<>();
        for (int writer = 0; writer < GROUP_COMMIT_STREAMS; writer++) {
            MutationStreamId stream = stream(FIRST_STATEMENT_ID, FIRST_WRITER_ID + writer);
            transaction.addStreams(IngestWire.encode(stream));
            batches.add(batch(stream, FIRST_SEQUENCE, new byte[] {(byte) writer}));
        }
        try (LocalMutationJournal journal = new LocalMutationJournal(
                directory, JOURNAL_PAYLOAD_LIMIT, JOURNAL_BYTE_LIMIT,
                JOURNAL_RECORD_LIMIT, GROUP_COMMIT_DELAY_MICROS);
                RetinaIngestParticipant participant = new RetinaIngestParticipant(
                        OWNER, journal, decisions, new NoOpInstaller(),
                        new IngestReadPins(READ_LEASE_MILLIS)))
        {
            participant.recover();
            decisions.transaction = transaction.build();
            for (MutationBatch entry : batches) {
                participant.append(entry);
            }
            long syncsBefore = journal.getSyncCount();
            CountDownLatch ready = new CountDownLatch(GROUP_COMMIT_STREAMS);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(GROUP_COMMIT_STREAMS);
            try {
                List<Future<?>> seals = new ArrayList<>();
                for (MutationBatch entry : batches) {
                    seals.add(executor.submit(() -> {
                        ready.countDown();
                        start.await();
                        participant.seal(seal(entry.getStreamId(), entry));
                        return null;
                    }));
                }
                assertTrue(ready.await(GROUP_COMMIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
                start.countDown();
                for (Future<?> result : seals) {
                    result.get(GROUP_COMMIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                assertTrue(journal.getSyncCount() - syncsBefore < GROUP_COMMIT_STREAMS);
                for (MutationBatch entry : batches) {
                    assertEquals(seal(entry.getStreamId(), entry),
                            journal.getSeal(entry.getStreamId()).orElseThrow(
                                    () -> new AssertionError("Missing durable stream seal")));
                }
            }
            finally {
                start.countDown();
                executor.shutdownNow();
            }
        }
    }

    @Test
    public void readsOnlyCompletedStatementsThroughImmutableFrontier() throws Exception
    {
        MutationStreamId firstStream = stream(FIRST_STATEMENT_ID, FIRST_WRITER_ID);
        MutationStreamId secondStream = stream(SECOND_STATEMENT_ID, SECOND_WRITER_ID);
        MutationBatch first = batch(firstStream, FIRST_SEQUENCE, FIRST_PAYLOAD);
        MutationBatch second = batch(firstStream, SECOND_SEQUENCE, SECOND_PAYLOAD);
        MutationBatch third = batch(secondStream, FIRST_SEQUENCE, THIRD_PAYLOAD);
        MutationStreamSeal firstSeal = seal(firstStream, first, second);
        MutationStreamSeal secondSeal = seal(secondStream, third);
        Transaction transaction = transaction(firstStream, secondStream, firstSeal, secondSeal);
        MutableDecisions decisions = new MutableDecisions();
        IngestReadPins pins = new IngestReadPins(READ_LEASE_MILLIS);

        try (LocalMutationJournal journal = new LocalMutationJournal(
                directory, JOURNAL_PAYLOAD_LIMIT, JOURNAL_BYTE_LIMIT, JOURNAL_RECORD_LIMIT);
                RetinaIngestParticipant participant = new RetinaIngestParticipant(
                        OWNER, journal, decisions, new NoOpInstaller(), pins,
                        CONFIGURED_BATCH_LIMIT, CONFIGURED_BYTE_LIMIT))
        {
            participant.recover();
            decisions.transaction = transaction;
            participant.append(first);
            participant.append(second);
            participant.append(third);
            participant.seal(firstSeal);
            participant.seal(secondSeal);

            ReadPin pin = participant.pinRead(ReadPin.newBuilder()
                    .setTransactionId(TRANSACTION_ID)
                    .setReadTimestamp(READ_TIMESTAMP)
                    .build());
            PrivateReadRequest request = request(pin.getToken());
            PrivateReadPage firstPage = participant.readPrivate(request);
            assertEquals(PAGE_BATCH_LIMIT, firstPage.getBatchesCount());
            assertArrayEquals(FIRST_PAYLOAD,
                    IngestWire.decode(firstPage.getBatches(0)).getPayload());
            assertArrayEquals(SECOND_PAYLOAD,
                    IngestWire.decode(firstPage.getBatches(1)).getPayload());
            assertFalse(firstPage.getEndOfInput());
            assertEquals(PAGE_BATCH_LIMIT, firstPage.getNextBatchOffset());

            PrivateReadPage secondPage = participant.readPrivate(request.toBuilder()
                    .setBatchOffset(firstPage.getNextBatchOffset())
                    .build());
            assertEquals(1, secondPage.getBatchesCount());
            assertArrayEquals(THIRD_PAYLOAD,
                    IngestWire.decode(secondPage.getBatches(0)).getPayload());
            assertTrue(secondPage.getEndOfInput());
            assertArrayEquals(firstPage.getManifestDigest().toByteArray(),
                    secondPage.getManifestDigest().toByteArray());

            assertThrows(IOException.class, () -> participant.readPrivate(request.toBuilder()
                    .setReadPinToken("wrong-token").build()));
            assertThrows(IOException.class, () -> participant.readPrivate(request.toBuilder()
                    .setReadOwnThroughOrdinal(FIRST_STATEMENT_ORDINAL).build()));
            assertThrows(IOException.class, () -> participant.readPrivate(request.toBuilder()
                    .setMaxBatches(CONFIGURED_BATCH_LIMIT + 1).build()));
            assertThrows(IOException.class, () -> participant.readPrivate(request.toBuilder()
                    .setMaxBytes(1).build()));
        }
    }

    private static PrivateReadRequest request(String pinToken)
    {
        return PrivateReadRequest.newBuilder()
                .setTransactionId(TRANSACTION_ID)
                .setReaderStatementId(READER_STATEMENT_ID)
                .setTableId(TABLE_ID)
                .setReadOwnThroughOrdinal(PRIVATE_READ_FRONTIER)
                .setReadPinToken(pinToken)
                .setBatchOffset(FIRST_SEQUENCE)
                .setMaxBatches(PAGE_BATCH_LIMIT)
                .setMaxBytes(CONFIGURED_BYTE_LIMIT)
                .build();
    }

    private static Transaction transaction(
            MutationStreamId firstStream,
            MutationStreamId secondStream,
            MutationStreamSeal firstSeal,
            MutationStreamSeal secondSeal)
    {
        StatementManifest first = completeStatement(
                FIRST_STATEMENT_ID, FIRST_STATEMENT_ORDINAL, firstSeal);
        StatementManifest second = completeStatement(
                SECOND_STATEMENT_ID, SECOND_STATEMENT_ORDINAL, secondSeal);
        StatementManifest reader = StatementManifest.newBuilder()
                .setStatementId(READER_STATEMENT_ID)
                .setQueryId("reader")
                .setOrdinal(READER_STATEMENT_ORDINAL)
                .setReadOwnThroughOrdinal(PRIVATE_READ_FRONTIER)
                .setTableId(TABLE_ID)
                .addReadTableIds(TABLE_ID)
                .setState(StatementState.STATEMENT_OPEN)
                .build();
        return Transaction.newBuilder()
                .setTransactionId(TRANSACTION_ID)
                .setRequestId("private-read")
                .setReadTimestamp(READ_TIMESTAMP)
                .setState(TransactionState.OPEN)
                .setTable(TABLE)
                .addEnlistedTables(TABLE)
                .addStreams(IngestWire.encode(firstStream))
                .addStreams(IngestWire.encode(secondStream))
                .addSeals(IngestWire.encode(firstSeal))
                .addSeals(IngestWire.encode(secondSeal))
                .addStatements(first)
                .addStatements(second)
                .addStatements(reader)
                .setScope(TransactionScope.EXPLICIT)
                .setRepresentation(WriteRepresentation.BUFFERED)
                .setAckMode(CommitAckMode.VISIBLE)
                .setOutcome(DecisionOutcome.UNDECIDED)
                .setProgress(PublicationProgress.PRIVATE)
                .build();
    }

    private static StatementManifest completeStatement(
            long statementId, long ordinal, MutationStreamSeal seal)
    {
        StatementManifest statement = StatementManifest.newBuilder()
                .setStatementId(statementId)
                .setQueryId("query-" + statementId)
                .setOrdinal(ordinal)
                .setReadOwnThroughOrdinal(ordinal - FIRST_STATEMENT_ORDINAL)
                .setTableId(TABLE_ID)
                .addReadTableIds(TABLE_ID)
                .setState(StatementState.STATEMENT_COMPLETE)
                .addSeals(IngestWire.encode(seal))
                .build();
        return statement.toBuilder()
                .setDigest(ByteString.copyFrom(IngestWire.statementDigest(statement)))
                .build();
    }

    private static MutationStreamId stream(long statementId, long writerId)
    {
        return new MutationStreamId(TRANSACTION_ID, statementId, writerId, TABLE_ID,
                SHARD_ID, MutationStreamId.Kind.APPEND_ROWS);
    }

    private static MutationBatch batch(
            MutationStreamId stream, long sequence, byte[] payload)
    {
        return new MutationBatch(stream, sequence, SCHEMA_VERSION,
                ColumnBatchCodec.FORMAT, ROWS_PER_BATCH, payload);
    }

    private static MutationStreamSeal seal(
            MutationStreamId stream, MutationBatch... batches)
    {
        byte[] digest = MutationStreamSeal.emptyDigest();
        long rows = 0;
        long bytes = 0;
        for (MutationBatch batch : batches) {
            digest = MutationStreamSeal.extendDigest(digest, batch.getDigest());
            rows = Math.addExact(rows, batch.getRowCount());
            bytes = Math.addExact(bytes, batch.getPayloadBytes());
        }
        return new MutationStreamSeal(stream, batches.length, rows, bytes, digest);
    }

    private static final class MutableDecisions
            implements RetinaIngestParticipant.Decisions
    {
        private Transaction transaction;

        public Transaction get(long transactionId) throws IOException
        {
            if (transaction == null || transaction.getTransactionId() != transactionId) {
                throw new IOException("Unknown transaction");
            }
            return transaction;
        }

        public Transaction abort(long transactionId) throws IOException
        {
            throw new IOException("Unexpected abort");
        }

        public TransactionList list(String owner)
        {
            TransactionList.Builder result = TransactionList.newBuilder()
                    .setPublishedTimestamp(READ_TIMESTAMP);
            if (transaction != null) {
                result.addTransactions(transaction);
            }
            return result.build();
        }
    }

    private static final class NoOpInstaller
            implements RetinaIngestParticipant.Installer
    {
        public void prepare(Transaction transaction, Iterable<MutationBatch> batches) {}

        public boolean install(
                Transaction transaction,
                Iterable<MutationBatch> batches,
                boolean recovering,
                boolean forceFileTail)
        {
            return true;
        }

        public void release(long transactionId) {}

        public void initializeRecovery(List<Transaction> transactions) {}

        public void close() {}
    }
}
