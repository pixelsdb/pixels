/*
 * Copyright 2026 PixelsDB.
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina.ingest;

import com.google.protobuf.ByteString;

import io.pixelsdb.pixels.common.ingest.*;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Exact stream receipts, authoritative decisions, and private preparation around existing Retina storage. */
public final class RetinaIngestParticipant implements Closeable {
    private static final long DEFAULT_INSTALL_POLL_MILLIS = 25L;
    private static final int DEFAULT_PRIVATE_READ_MAX_BATCHES = 128;
    private static final int DEFAULT_PRIVATE_READ_MAX_BYTES = 4 * 1024 * 1024;
    private static final Logger LOG = LogManager.getLogger(RetinaIngestParticipant.class);
    public interface Decisions {
        Transaction get(long id) throws Exception;

        Transaction abort(long id) throws Exception;

        TransactionList list(String owner) throws Exception;

        long publishedTimestamp() throws Exception;
    }

    public interface Installer extends Closeable {
        void prepare(Transaction tx, Iterable<MutationBatch> batches) throws Exception;

        boolean install(
                Transaction tx,
                Iterable<MutationBatch> batches,
                boolean recovering,
                boolean forceFileTail)
                throws Exception;

        default long installPollMillis() {
            return DEFAULT_INSTALL_POLL_MILLIS;
        }

        void release(long txId) throws Exception;

        void initializeRecovery(List<Transaction> transactions) throws Exception;

        default boolean recoveredByCheckpoint(Transaction tx) throws Exception {
            return false;
        }

        default boolean checkpoint(Transaction tx, Iterable<MutationBatch> batches)
                throws Exception {
            return false;
        }
    }

    private final String owner;
    private final LocalMutationJournal journal;
    private final Decisions decisions;
    private final Installer installer;
    private final IngestReadPins readPins;
    private final int privateReadMaxBatches;
    private final int privateReadMaxBytes;
    private final Map<Long, ByteString> prepared = new HashMap<>();
    private final Set<Long> installed = new HashSet<>();
    private final Set<Long> installing = new HashSet<>();
    private long lastInstalledTimestamp;
    private boolean ready;
    private final ScheduledExecutorService checkpointWorker =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "pixels-ingest-checkpoint");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final AtomicBoolean checkpointWorkerStarted = new AtomicBoolean();

    public RetinaIngestParticipant(
            String owner,
            LocalMutationJournal journal,
            Decisions decisions,
            Installer installer,
            IngestReadPins readPins) {
        this(owner, journal, decisions, installer, readPins,
                DEFAULT_PRIVATE_READ_MAX_BATCHES, DEFAULT_PRIVATE_READ_MAX_BYTES);
    }

    public RetinaIngestParticipant(
            String owner,
            LocalMutationJournal journal,
            Decisions decisions,
            Installer installer,
            IngestReadPins readPins,
            int privateReadMaxBatches,
            int privateReadMaxBytes) {
        if (privateReadMaxBatches <= 0 || privateReadMaxBytes <= 0) {
            throw new IllegalArgumentException("Private-read limits must be positive");
        }
        this.owner = owner;
        this.journal = journal;
        this.decisions = decisions;
        this.installer = installer;
        this.readPins = readPins;
        this.privateReadMaxBatches = privateReadMaxBatches;
        this.privateReadMaxBytes = privateReadMaxBytes;
    }

    private void serving() throws IOException {
        if (!ready) {
            throw new IOException("Retina ingest participant is not ready");
        }
    }

    private void owns(Transaction tx, MutationStreamId stream) throws IOException {
        TableSpec table = IngestWire.table(tx, stream.getTableId());
        if (stream.getTransactionId() != tx.getTransactionId()
                || !IngestWire.owner(IngestWire.route(table, stream.getShardId()))
                        .equals(owner)
                || !tx.getStreamsList().contains(IngestWire.encode(stream))) {
            throw new IOException("Unregistered or incorrectly routed stream");
        }
    }

    public synchronized void append(MutationBatch batch) throws Exception {
        serving();
        Transaction tx = decisions.get(batch.getStreamId().getTransactionId());
        owns(tx, batch.getStreamId());
        if (tx.getState() != TransactionState.OPEN) {
            throw new IOException("Transaction no longer accepts input");
        }
        TableSpec table = IngestWire.table(tx, batch.getStreamId().getTableId());
        if (batch.getSchemaVersion() != table.getSchemaVersion()
                || batch.getPayloadFormat()
                        != io.pixelsdb.pixels.common.ingest.wire.ColumnBatchCodec.FORMAT
                || batch.getStreamId().getKind() != MutationStreamId.Kind.APPEND_ROWS) {
            throw new IOException("Unsupported batch kind, codec, or schema version");
        }
        journal.append(batch);
    }

    public MutationStreamSeal seal(MutationStreamSeal seal) throws Exception {
        synchronized (this) {
            serving();
            Transaction tx = decisions.get(seal.getStreamId().getTransactionId());
            owns(tx, seal.getStreamId());
            if (tx.getState() == TransactionState.ABORTED) {
                throw new IOException("Transaction aborted");
            }
        }
        // The journal serializes stream state and syncs a durable prefix. Do not hold the
        // participant lock through that sync: independent streams can share one WAL fsync.
        journal.seal(seal);
        return seal;
    }

    private Transaction authoritative(Transaction requested) throws Exception {
        Transaction current = decisions.get(requested.getTransactionId());
        if (!current.getTable().equals(requested.getTable())
                || !current.getEnlistedTablesList().equals(requested.getEnlistedTablesList())
                || !current.getStatementsList().equals(requested.getStatementsList())
                || current.getRepresentation() != requested.getRepresentation()
                || current.getAckMode() != requested.getAckMode()
                || !current.getSealsList().equals(requested.getSealsList())) {
            throw new IOException("Participant manifest differs from authoritative transaction");
        }
        if (!IngestWire.owners(current).contains(owner)) {
            throw new IOException("This node is not a transaction participant");
        }
        return current;
    }

    private Iterable<MutationBatch> batches(Transaction tx) throws Exception {
        List<StreamSeal> seals = IngestWire.localSeals(tx, owner);
        for (StreamSeal expected : seals) {
            MutationStreamSeal receipt =
                    journal.getSeal(IngestWire.decode(expected.getStream()))
                            .orElseThrow(() -> new IOException("Missing stream seal"));
            if (!IngestWire.decode(expected).equals(receipt)) {
                throw new IOException("Missing durable stream seal");
            }
        }
        // Re-iterable, bounded replay: retain descriptors, not the transaction's entire payload.
        return () ->
                new Iterator<MutationBatch>() {
                    private int stream;
                    private long sequence;

                    public boolean hasNext() {
                        while (stream < seals.size()
                                && sequence >= seals.get(stream).getBatchCount()) {
                            stream++;
                            sequence = 0;
                        }
                        return stream < seals.size();
                    }

                    public MutationBatch next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        try {
                            return journal.readSealedBatch(
                                    IngestWire.decode(seals.get(stream).getStream()), sequence++);
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    }
                };
    }

    public synchronized PrepareToken prepare(Transaction request) throws Exception {
        serving();
        Transaction tx = authoritative(request);
        if (tx.getState() != TransactionState.SEALED
                && tx.getState() != TransactionState.PREPARED) {
            throw new IOException("Transaction is not in a preparable state");
        }
        ByteString digest = ByteString.copyFrom(IngestWire.prepareDigest(tx, owner));
        ByteString old = prepared.get(tx.getTransactionId());
        if (old != null && !old.equals(digest)) {
            throw new IOException("Prepared digest mismatch");
        }
        if (old == null) {
            Iterable<MutationBatch> batches = batches(tx);
            try {
                installer.prepare(tx, batches);
                prepared.put(tx.getTransactionId(), digest);
            } catch (Exception e) {
                installer.release(tx.getTransactionId());
                throw e;
            }
        }
        return PrepareToken.newBuilder().setOwner(owner).setDigest(digest).build();
    }

    public boolean install(Transaction request, boolean forceFileTail) throws Exception {
        Transaction tx;
        synchronized (this) {
            serving();
            tx = authoritative(request);
            while (installing.contains(tx.getTransactionId())) {
                wait();
            }
            if (installed.contains(tx.getTransactionId())) {
                return true;
            }
            validateInstallation(tx);
            installing.add(tx.getTransactionId());
        }
        boolean readyForPublication = false;
        try {
            readyForPublication = installer.install(
                    tx, batches(tx), false, forceFileTail);
            return readyForPublication;
        } finally {
            synchronized (this) {
                installing.remove(tx.getTransactionId());
                if (readyForPublication) {
                    installed.add(tx.getTransactionId());
                    lastInstalledTimestamp =
                            Math.max(lastInstalledTimestamp, tx.getCommitTimestamp());
                }
                notifyAll();
            }
        }
    }

    private void installAuthorized(Transaction tx, boolean recovering) throws Exception {
        if (installed.contains(tx.getTransactionId())) {
            return;
        }
        if (recovering && installer.recoveredByCheckpoint(tx)) {
            installed.add(tx.getTransactionId());
            lastInstalledTimestamp = Math.max(lastInstalledTimestamp, tx.getCommitTimestamp());
            return;
        }
        validateInstallation(tx);
        while (!installer.install(tx, batches(tx), recovering, false)) {
            Thread.sleep(installer.installPollMillis());
        }
        installed.add(tx.getTransactionId());
        lastInstalledTimestamp = Math.max(lastInstalledTimestamp, tx.getCommitTimestamp());
    }

    private void validateInstallation(Transaction tx) throws IOException {
        if (!IngestWire.committed(tx)) {
            throw new IOException("Installation requires authoritative COMMIT");
        }
        boolean validToken = false;
        for (PrepareToken token : tx.getTokensList()) {
            if (token.getOwner().equals(owner)
                    && token.getDigest()
                            .equals(ByteString.copyFrom(IngestWire.prepareDigest(tx, owner)))) {
                validToken = true;
            }
        }
        if (!validToken) {
            throw new IOException("Committed transaction lacks the participant prepare token");
        }
    }

    public synchronized void discard(long id) throws Exception {
        if (!ready) {
            return;
        } // Recovery resolves decisions before accepting work.
        Transaction tx = decisions.get(id);
        if (tx.getState() == TransactionState.ABORTED) {
            journal.discardAbortedTransaction(id);
            installer.release(id);
            prepared.remove(id);
        } else if (tx.getState() == TransactionState.PUBLISHED) {
            installer.release(id);
            prepared.remove(id);
        }
        // COMMIT_DECIDED retains reservations until global publication.
    }

    public synchronized void recover() throws Exception {
        ready = false;
        List<Transaction> transactions =
                new ArrayList<>(decisions.list(owner).getTransactionsList());
        // No local timer may release a PREPARED transaction that has already committed.
        for (int i = 0; i < transactions.size(); i++) {
            Transaction tx = transactions.get(i);
            if (!IngestWire.committed(tx) && tx.getState() != TransactionState.ABORTED) {
                transactions.set(i, decisions.abort(tx.getTransactionId()));
            }
        }
        installer.initializeRecovery(transactions);
        transactions.sort(Comparator.comparingLong(Transaction::getCommitTimestamp));
        for (Transaction tx : transactions) {
            if (tx.getState() == TransactionState.ABORTED) {
                journal.discardAbortedTransaction(tx.getTransactionId());
            } else if (IngestWire.committed(tx)) {
                installAuthorized(tx, true);
            }
        }
        ready = true;
        readPins.ready();
        startCheckpointWorker();
    }

    private void startCheckpointWorker() {
        if (!checkpointWorkerStarted.compareAndSet(false, true)) {
            return;
        }
        checkpointWorker.scheduleWithFixedDelay(
                () -> {
                    try {
                        checkpointPublishedTransactions();
                    } catch (Exception e) {
                        // File publication and RecoveryCheckpoint advancement are asynchronous.
                        // The next pass retries without weakening recovery ownership.
                        LOG.debug("Ingest checkpoint is not ready; retrying: {}", e.toString());
                    }
                }, 1, 1, TimeUnit.SECONDS);
    }

    void checkpointPublishedTransactions() throws Exception {
        List<Transaction> transactions;
        synchronized (this) {
            if (!ready) {
                return;
            }
            transactions = new ArrayList<>();
            for (Transaction tx : decisions.list(owner).getTransactionsList()) {
                if (tx.getState() == TransactionState.PUBLISHED
                        && installed.contains(tx.getTransactionId())) {
                    transactions.add(tx);
                }
            }
        }
        for (Transaction tx : transactions) {
            if (installer.recoveredByCheckpoint(tx)) {
                journal.checkpointTransaction(tx.getTransactionId());
                continue;
            }
            if (installer.checkpoint(tx, batches(tx))) {
                journal.checkpointTransaction(tx.getTransactionId());
            }
        }
        journal.compactRetiredTransactions();
    }

    public void checkpoint(long transactionId) throws Exception {
        Transaction tx;
        synchronized (this) {
            serving();
            tx = decisions.get(transactionId);
            if (tx.getState() != TransactionState.PUBLISHED
                    || !installed.contains(transactionId)) {
                throw new IOException(
                        "Transaction is not installed and PUBLISHED");
            }
        }
        if (!installer.recoveredByCheckpoint(tx)
                && !installer.checkpoint(tx, batches(tx))) {
            throw new IOException("Transaction recovery checkpoint is not durable yet");
        }
        journal.checkpointTransaction(transactionId);
    }

    public ReadPin pinRead(ReadPin request) throws Exception {
        synchronized (this) {
            serving();
        }
        // Publication is monotonic; fetch only its watermark without holding up
        // unrelated transaction admission while the coordinator RPC completes.
        long publishedTimestamp = decisions.publishedTimestamp();
        if (request.getReadTimestamp() > publishedTimestamp) {
            throw new IOException("Cannot pin an unpublished read timestamp");
        }
        synchronized (this) {
            serving();
            return readPins.pin(request);
        }
    }

    public synchronized PrivateReadPage readPrivate(PrivateReadRequest request) throws Exception {
        serving();
        if (request.getMaxBatches() == 0
                || request.getMaxBatches() > privateReadMaxBatches
                || request.getMaxBytes() == 0
                || request.getMaxBytes() > privateReadMaxBytes) {
            throw new IOException("Private-read page exceeds configured limits");
        }
        Transaction tx = decisions.get(request.getTransactionId());
        if (tx.getState() != TransactionState.OPEN || tx.getRollbackOnly()) {
            throw new IOException("Private reads require an open transaction");
        }
        readPins.validate(request.getReadPinToken(), tx.getReadTimestamp(),
                tx.getTransactionId());
        StatementManifest reader = null;
        for (StatementManifest statement : tx.getStatementsList()) {
            if (statement.getStatementId() == request.getReaderStatementId()) {
                reader = statement;
                break;
            }
        }
        if (reader == null
                || reader.getState() != StatementState.STATEMENT_OPEN
                || reader.getReadOwnThroughOrdinal() != request.getReadOwnThroughOrdinal()
                || !reader.getReadTableIdsList().contains(request.getTableId())) {
            throw new IOException("Private-read frontier does not match the open statement");
        }
        IngestWire.table(tx, request.getTableId());
        List<StreamSeal> seals = new ArrayList<>();
        for (StatementManifest statement : tx.getStatementsList()) {
            if (statement.getState() != StatementState.STATEMENT_COMPLETE
                    || statement.getOrdinal() > request.getReadOwnThroughOrdinal()
                    || statement.getTableId() != request.getTableId()) {
                continue;
            }
            for (StreamSeal seal : statement.getSealsList()) {
                TableSpec table = IngestWire.table(tx, seal.getStream().getTableId());
                if (IngestWire.owner(IngestWire.route(
                        table, seal.getStream().getShardId())).equals(owner)) {
                    seals.add(seal);
                }
            }
        }
        seals.sort(Comparator
                .comparingLong((StreamSeal seal) -> seal.getStream().getStatementId())
                .thenComparingLong(seal -> seal.getStream().getWriterId())
                .thenComparingInt(seal -> seal.getStream().getShardId())
                .thenComparingInt(seal -> seal.getStream().getKindValue()));

        long totalBatches = 0;
        for (StreamSeal seal : seals) {
            totalBatches = Math.addExact(totalBatches, seal.getBatchCount());
        }
        if (request.getBatchOffset() > totalBatches) {
            throw new IOException("Private-read offset exceeds the exact manifest");
        }

        PrivateReadPage.Builder page = PrivateReadPage.newBuilder()
                .setManifestDigest(ByteString.copyFrom(IngestWire.privateReadDigest(
                        tx, request.getReaderStatementId(), request.getTableId(),
                        request.getReadOwnThroughOrdinal())));
        long logicalOffset = 0;
        int pageBytes = 0;
        for (StreamSeal seal : seals) {
            MutationStreamId stream = IngestWire.decode(seal.getStream());
            MutationStreamSeal durableSeal = journal.getSeal(stream)
                    .orElseThrow(() -> new IOException("Missing private stream seal"));
            if (!durableSeal.equals(IngestWire.decode(seal))) {
                throw new IOException("Private stream seal differs from exact manifest");
            }
            for (long sequence = 0; sequence < seal.getBatchCount(); sequence++) {
                if (logicalOffset++ < request.getBatchOffset()) {
                    continue;
                }
                AppendRequest batch = IngestWire.encode(journal.readSealedBatch(stream, sequence));
                int nextBytes = Math.addExact(pageBytes, batch.getSerializedSize());
                if (page.getBatchesCount() >= request.getMaxBatches()
                        || nextBytes > request.getMaxBytes()) {
                    if (page.getBatchesCount() == 0) {
                        throw new IOException(
                                "Private batch exceeds the requested page byte limit");
                    }
                    return page.setNextBatchOffset(
                            request.getBatchOffset() + page.getBatchesCount())
                            .setEndOfInput(false)
                            .build();
                }
                page.addBatches(batch);
                pageBytes = nextBytes;
            }
        }
        return page.setNextBatchOffset(totalBatches).setEndOfInput(true).build();
    }

    public IngestReadPins readPins() {
        return readPins;
    }

    public synchronized boolean isReady() {
        return ready;
    }

    @Override
    public synchronized void close() throws IOException {
        ready = false;
        checkpointWorker.shutdownNow();
        try {
            installer.close();
        } finally {
            journal.close();
        }
    }
}
