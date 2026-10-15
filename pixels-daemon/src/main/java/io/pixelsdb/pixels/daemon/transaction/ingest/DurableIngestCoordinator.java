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
package io.pixelsdb.pixels.daemon.transaction.ingest;

import com.google.protobuf.ByteString;

import io.pixelsdb.pixels.common.ingest.durable.AtomicStateFile;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongSupplier;

/**
 * Single-owner LOCAL-durability transaction coordinator. The locked state volume is
 * authoritative. A COMMIT is neither reported nor remotely installed before its decision
 * reaches the synchronized state-log prefix.
 * Endpoint topology is frozen in the first publication snapshot and cannot migrate silently.
 */
public final class DurableIngestCoordinator implements Closeable {
    private static final int COORDINATOR_SNAPSHOT_VERSION = 2;
    private static final int MAX_IDENTITY_CHARACTERS = 512;
    private static final int MAX_TCP_PORT = 65_535;
    private static final int RETINA_TIMESTAMP_BITS = 48;
    private static final long MAX_RETINA_TIMESTAMP = 1L << RETINA_TIMESTAMP_BITS;
    private static final long MIN_VISIBILITY_POLL_MILLIS = 1L;
    private static final long MAX_VISIBILITY_POLL_MILLIS = 25L;
    private static final long FIRST_STATEMENT_ORDINAL = 1L;
    private static final long EMPTY_PRIVATE_READ_FRONTIER = 0L;
    private static final long DEFAULT_TERMINAL_RETENTION_MILLIS = TimeUnit.DAYS.toMillis(1);
    private static final int DEFAULT_MAX_TERMINAL_TRANSACTIONS = 100_000;
    private static final int DEFAULT_INSTALLATION_THREADS = 16;
    private static final long MINIMUM_RETIREMENT_TIMESTAMP_MILLIS = 1L;
    private static final long RECONCILIATION_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1L);
    private static final long DEFAULT_GROUP_COMMIT_DELAY_MICROS = 0L;
    private static final long MAX_GROUP_COMMIT_DELAY_MICROS = TimeUnit.SECONDS.toMicros(1L);

    public interface Tables {
        TableSpec load(String schema, String table) throws Exception;

        List<Route> routes() throws Exception;
    }

    public interface Participants {
        PrepareToken prepare(String owner, Transaction transaction) throws Exception;

        boolean install(String owner, Transaction transaction, boolean forceFileTail)
                throws Exception;

        default void checkpoint(String owner, long transactionId) throws Exception {
            throw new IOException("Participant checkpoint acknowledgement is unavailable");
        }

        void discard(String owner, long transactionId) throws Exception;
    }

    public interface StateStore extends Closeable {
        enum Durability {
            DEFERRED,
            SYNCHRONIZED
        }

        CoordinatorSnapshot read() throws IOException;

        void store(CoordinatorSnapshot snapshot, Durability durability) throws IOException;

        void synchronize() throws IOException;

        default void store(CoordinatorSnapshot snapshot) throws IOException {
            store(snapshot, Durability.SYNCHRONIZED);
        }
    }

    private static final class AtomicSnapshotStore implements StateStore {
        private final AtomicStateFile delegate;

        private AtomicSnapshotStore(AtomicStateFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public CoordinatorSnapshot read() throws IOException {
            byte[] bytes = delegate.read();
            return bytes.length == 0 ? null : CoordinatorSnapshot.parseFrom(bytes);
        }

        @Override
        public void store(CoordinatorSnapshot value, Durability durability) throws IOException {
            delegate.store(value.toByteArray());
        }

        @Override
        public void synchronize() {}

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final Logger LOG = LogManager.getLogger(DurableIngestCoordinator.class);
    private final StateStore store;
    private final Tables tables;
    private final Participants participants;
    private final LongSupplier ids;
    private final Clock clock;
    private final long leaseMillis;
    private final int maxTransactions;
    private final int maxStreams;
    private final long terminalRetentionMillis;
    private final int maxTerminalTransactions;
    private final long groupCommitDelayNanos;
    private final Object publisher = new Object();
    private final ScheduledExecutorService recovery;
    private final ExecutorService installationExecutor;
    private final Map<Long, Future<Boolean>> installationTasks = new HashMap<>();
    private final Set<Long> installationContributed = new HashSet<>();
    private final Set<Long> installationReady = new HashSet<>();
    private CoordinatorSnapshot snapshot;
    private long stateSequence;
    private long synchronizedSequence;
    private long publicationSequence;
    private boolean stateSyncInProgress;
    private IOException persistenceFailure;
    private long forceFileThroughTimestamp;
    private volatile boolean closed;

    public DurableIngestCoordinator(
            AtomicStateFile store,
            Tables tables,
            Participants participants,
            LongSupplier ids,
            Clock clock,
            long baselineTimestamp,
            long leaseMillis,
            int maxTransactions,
            int maxStreams)
            throws IOException {
        this(store, tables, participants, ids, clock, baselineTimestamp, leaseMillis,
                maxTransactions, maxStreams, DEFAULT_TERMINAL_RETENTION_MILLIS,
                DEFAULT_MAX_TERMINAL_TRANSACTIONS, DEFAULT_INSTALLATION_THREADS);
    }

    public DurableIngestCoordinator(
            AtomicStateFile store,
            Tables tables,
            Participants participants,
            LongSupplier ids,
            Clock clock,
            long baselineTimestamp,
            long leaseMillis,
            int maxTransactions,
            int maxStreams,
            long terminalRetentionMillis,
            int maxTerminalTransactions)
            throws IOException {
        this(store, tables, participants, ids, clock, baselineTimestamp, leaseMillis,
                maxTransactions, maxStreams, terminalRetentionMillis,
                maxTerminalTransactions, DEFAULT_INSTALLATION_THREADS);
    }

    public DurableIngestCoordinator(
            AtomicStateFile store,
            Tables tables,
            Participants participants,
            LongSupplier ids,
            Clock clock,
            long baselineTimestamp,
            long leaseMillis,
            int maxTransactions,
            int maxStreams,
            long terminalRetentionMillis,
            int maxTerminalTransactions,
            int installationThreads)
            throws IOException {
        this(new AtomicSnapshotStore(store), tables, participants, ids, clock,
                baselineTimestamp, leaseMillis, maxTransactions, maxStreams,
                terminalRetentionMillis, maxTerminalTransactions, installationThreads);
    }

    public DurableIngestCoordinator(
            StateStore store,
            Tables tables,
            Participants participants,
            LongSupplier ids,
            Clock clock,
            long baselineTimestamp,
            long leaseMillis,
            int maxTransactions,
            int maxStreams,
            long terminalRetentionMillis,
            int maxTerminalTransactions,
            int installationThreads)
            throws IOException {
        this(store, tables, participants, ids, clock, baselineTimestamp, leaseMillis,
                maxTransactions, maxStreams, terminalRetentionMillis,
                maxTerminalTransactions, installationThreads,
                DEFAULT_GROUP_COMMIT_DELAY_MICROS);
    }

    public DurableIngestCoordinator(
            StateStore store,
            Tables tables,
            Participants participants,
            LongSupplier ids,
            Clock clock,
            long baselineTimestamp,
            long leaseMillis,
            int maxTransactions,
            int maxStreams,
            long terminalRetentionMillis,
            int maxTerminalTransactions,
            int installationThreads,
            long groupCommitDelayMicros)
            throws IOException {
        this.store = store;
        this.tables = tables;
        this.participants = participants;
        this.ids = ids;
        this.clock = clock;
        this.leaseMillis = leaseMillis;
        this.maxTransactions = maxTransactions;
        this.maxStreams = maxStreams;
        if (terminalRetentionMillis < leaseMillis
                || maxTerminalTransactions <= 0
                || installationThreads <= 0
                || groupCommitDelayMicros < 0L
                || groupCommitDelayMicros > MAX_GROUP_COMMIT_DELAY_MICROS) {
            throw new IllegalArgumentException("Invalid coordinator limits");
        }
        this.terminalRetentionMillis = terminalRetentionMillis;
        this.maxTerminalTransactions = maxTerminalTransactions;
        this.groupCommitDelayNanos = TimeUnit.MICROSECONDS.toNanos(groupCommitDelayMicros);
        CoordinatorSnapshot recovered = store.read();
        if (recovered == null) {
            snapshot =
                    CoordinatorSnapshot.newBuilder()
                            .setVersion(COORDINATOR_SNAPSHOT_VERSION)
                            .setPublishedTimestamp(baselineTimestamp)
                            .setLastCommitTimestamp(baselineTimestamp)
                            .build();
            store.store(snapshot);
            stateSequence = 1L;
            synchronizedSequence = stateSequence;
            publicationSequence = stateSequence;
        } else {
            snapshot = recovered;
            validateRecoveredSnapshot(snapshot);
        }
        recovery =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r, "pixels-ingest-decisions");
                            thread.setDaemon(true);
                            return thread;
                        });
        installationExecutor = Executors.newFixedThreadPool(
                installationThreads,
                runnable -> {
                    Thread thread = new Thread(runnable, "pixels-ingest-install");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    public void start() {
        recovery.scheduleWithFixedDelay(
                () -> {
                    if (closed) {
                        return;
                    }
                    try {
                        reconcileOnce();
                    } catch (Exception e) {
                        LOG.warn("Ingest reconciliation will retry: {}", e.toString());
                    }
                },
                0L,
                RECONCILIATION_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    void reconcileOnce() throws Exception {
        expire();
        drivePublication();
        cleanupTerminalReservations();
        pruneTerminalFences();
    }

    private void checkOpen() throws IOException {
        if (persistenceFailure != null) {
            throw new IOException("Coordinator state unavailable; recovery required",
                    persistenceFailure);
        }
        if (closed) {
            throw new IOException("Coordinator closed");
        }
    }

    private void save(CoordinatorSnapshot value) throws IOException {
        appendState(value);
        synchronizeState(stateSequence);
    }

    private void appendState(CoordinatorSnapshot value) throws IOException {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException("Coordinator state lock is not held");
        }
        boolean changesPublication = value.getPublishedTimestamp() != snapshot.getPublishedTimestamp()
                || !value.getRoutesList().equals(snapshot.getRoutesList());
        store.store(value, StateStore.Durability.DEFERRED);
        snapshot = value;
        stateSequence = Math.addExact(stateSequence, 1L);
        if (changesPublication) {
            publicationSequence = stateSequence;
        }
    }

    private void saveDeferred(CoordinatorSnapshot value) throws IOException {
        appendState(value);
    }

    private void synchronizeState(long requiredSequence) throws IOException {
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException("Coordinator state lock is not held");
        }
        boolean interrupted = false;
        try {
            while (synchronizedSequence < requiredSequence) {
                if (persistenceFailure != null) {
                    throw new IOException("Coordinator state synchronization failed",
                            persistenceFailure);
                }
                if (!stateSyncInProgress) {
                    stateSyncInProgress = true;
                    try {
                        if (groupCommitDelayNanos > 0L) {
                            long millis = TimeUnit.NANOSECONDS.toMillis(groupCommitDelayNanos);
                            int nanos = (int) (groupCommitDelayNanos
                                    - TimeUnit.MILLISECONDS.toNanos(millis));
                            try {
                                wait(millis, nanos);
                            }
                            catch (InterruptedException ignored) {
                                interrupted = true;
                            }
                        }
                        long synchronizedThrough = stateSequence;
                        store.synchronize();
                        synchronizedSequence = synchronizedThrough;
                    }
                    catch (IOException e) {
                        persistenceFailure = e;
                        throw e;
                    }
                    finally {
                        stateSyncInProgress = false;
                        notifyAll();
                    }
                }
                else {
                    try {
                        wait();
                    }
                    catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
            }
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void validateRecoveredSnapshot(CoordinatorSnapshot recovered)
            throws IOException {
        if (recovered.getVersion() != COORDINATOR_SNAPSHOT_VERSION
                || recovered.getPublishedTimestamp() > recovered.getLastCommitTimestamp()
                || recovered.getLastCommitTimestamp() >= MAX_RETINA_TIMESTAMP) {
            throw new IOException("Invalid transaction checkpoint");
        }
        validateRoutes(recovered.getRoutesList());
        Set<Long> transactionIds = new HashSet<>();
        Set<String> requestIds = new HashSet<>();
        Set<Long> commitTimestamps = new HashSet<>();
        for (Transaction transaction : recovered.getTransactionsList()) {
            validateRecoveredTransaction(transaction, recovered, false);
            validateUniqueTransactionIdentity(
                    transaction, transactionIds, requestIds, commitTimestamps);
        }
        for (TerminalTransaction terminal : recovered.getTerminalTransactionsList()) {
            Transaction transaction = terminal.getTransaction();
            if (terminal.getRetiredAtMillis() < MINIMUM_RETIREMENT_TIMESTAMP_MILLIS
                    || transaction.getTransactionId()
                            > recovered.getRetiredTransactionIdHighWatermark()) {
                throw new IOException("Invalid terminal transaction fence");
            }
            validateRecoveredTransaction(transaction, recovered, true);
            validateUniqueTransactionIdentity(
                    transaction, transactionIds, requestIds, commitTimestamps);
        }
    }

    private static void validateUniqueTransactionIdentity(
            Transaction transaction,
            Set<Long> transactionIds,
            Set<String> requestIds,
            Set<Long> commitTimestamps)
            throws IOException {
        if (!transactionIds.add(transaction.getTransactionId())
                || !requestIds.add(transaction.getRequestId())) {
            throw new IOException("Duplicate transaction identity in checkpoint");
        }
        if (IngestWire.committed(transaction)
                && !commitTimestamps.add(transaction.getCommitTimestamp())) {
            throw new IOException("Duplicate commit timestamp in checkpoint");
        }
    }

    private static void validateRecoveredTransaction(
            Transaction transaction, CoordinatorSnapshot recovered, boolean terminal)
            throws IOException {
        if (transaction.getTransactionId() <= 0
                || transaction.getRequestId().isEmpty()
                || transaction.getRequestId().length() > MAX_IDENTITY_CHARACTERS
                || transaction.getReadTimestamp() > recovered.getPublishedTimestamp()
                || transaction.getExpiresAtMillis() <= 0
                || transaction.getScope() == TransactionScope.UNRECOGNIZED
                || transaction.getRepresentation() == WriteRepresentation.UNRECOGNIZED
                || transaction.getAckMode() == CommitAckMode.UNRECOGNIZED
                || transaction.getState() == TransactionState.UNRECOGNIZED
                || transaction.getOutcome() == DecisionOutcome.UNRECOGNIZED
                || transaction.getProgress() == PublicationProgress.UNRECOGNIZED) {
            throw new IOException("Invalid transaction identity or mode in checkpoint");
        }

        Map<Long, TableSpec> tables = validateTables(transaction, recovered.getRoutesList());
        Map<Long, StatementManifest> statements = validateStatements(transaction, tables, terminal);
        if (terminal) {
            validateTerminalTransaction(transaction, recovered);
            return;
        }

        validateWriters(transaction, statements);
        validateStreamsAndSeals(transaction, statements, tables);
        validateDecisionState(transaction, recovered);
        validatePrepareTokens(transaction);
    }

    private static Map<Long, TableSpec> validateTables(
            Transaction transaction, List<Route> checkpointRoutes) throws IOException {
        if (transaction.getEnlistedTablesCount() == 0) {
            throw new IOException("Transaction has no enlisted tables");
        }
        Map<Long, TableSpec> tables = new HashMap<>();
        Set<String> names = new HashSet<>();
        for (TableSpec table : transaction.getEnlistedTablesList()) {
            if (table.getTableId() <= 0
                    || table.getSchemaName().isEmpty()
                    || table.getTableName().isEmpty()
                    || !tables.containsKey(table.getTableId())
                            && !names.add(table.getSchemaName() + "\u0000" + table.getTableName())) {
                throw new IOException("Invalid enlisted table in checkpoint");
            }
            if (tables.put(table.getTableId(), table) != null) {
                throw new IOException("Duplicate enlisted table in checkpoint");
            }
            validateRoutes(table.getRoutesList());
            if (!checkpointRoutes.isEmpty() && !checkpointRoutes.equals(table.getRoutesList())) {
                throw new IOException("Enlisted table topology differs from checkpoint topology");
            }
        }
        TableSpec primary = tables.get(transaction.getTable().getTableId());
        if (primary == null || !primary.equals(transaction.getTable())) {
            throw new IOException("Primary transaction table is not exactly enlisted");
        }
        return tables;
    }

    private static void validateRoutes(List<Route> routes) throws IOException {
        Set<Integer> shardIds = new HashSet<>();
        for (Route route : routes) {
            if (!shardIds.add(route.getShardId())
                    || route.getHost().isEmpty()
                    || route.getPort() <= 0
                    || route.getPort() > MAX_TCP_PORT) {
                throw new IOException("Invalid or duplicate route in checkpoint");
            }
        }
    }

    private static Map<Long, StatementManifest> validateStatements(
            Transaction transaction, Map<Long, TableSpec> tables, boolean terminal)
            throws IOException {
        if (transaction.getStatementsCount() == 0) {
            throw new IOException("Transaction has no statements");
        }
        Map<Long, StatementManifest> statements = new HashMap<>();
        boolean openStatementSeen = false;
        long expectedOrdinal = FIRST_STATEMENT_ORDINAL;
        for (StatementManifest statement : transaction.getStatementsList()) {
            try {
                validateStatementIdentity(statement.getStatementId(), statement.getQueryId(),
                        statement.getOrdinal(), statement.getReadOwnThroughOrdinal());
            }
            catch (IllegalArgumentException e) {
                throw new IOException("Invalid statement identity in checkpoint", e);
            }
            if (statement.getOrdinal() != expectedOrdinal
                    || statement.getReadOwnThroughOrdinal()
                            != expectedOrdinal - FIRST_STATEMENT_ORDINAL
                    || statements.put(statement.getStatementId(), statement) != null
                    || statement.getState() == StatementState.UNRECOGNIZED) {
                throw new IOException("Invalid statement sequence in checkpoint");
            }
            expectedOrdinal++;
            Set<Long> readTables = new HashSet<>();
            if (statement.getReadTableIdsCount() == 0) {
                throw new IOException("Statement has no enlisted read table");
            }
            for (long tableId : statement.getReadTableIdsList()) {
                if (!tables.containsKey(tableId) || !readTables.add(tableId)) {
                    throw new IOException("Invalid statement read table in checkpoint");
                }
            }
            if (statement.getTableId() != 0
                    && (!tables.containsKey(statement.getTableId())
                            || !readTables.contains(statement.getTableId()))) {
                throw new IOException("Statement write table is not exactly enlisted");
            }
            if (statement.getState() == StatementState.STATEMENT_OPEN) {
                if ((terminal && transaction.getState() != TransactionState.ABORTED)
                        || openStatementSeen
                        || statement.getOrdinal() != transaction.getStatementsCount()
                        || statement.getSealsCount() != 0
                        || !statement.getDigest().isEmpty()) {
                    throw new IOException("Invalid open statement in checkpoint");
                }
                openStatementSeen = true;
            }
            else if (!terminal && !statement.getDigest().equals(ByteString.copyFrom(
                    IngestWire.statementDigest(statement)))) {
                throw new IOException("Statement manifest digest mismatch in checkpoint");
            }
        }
        if (transaction.getState() != TransactionState.OPEN
                && transaction.getState() != TransactionState.ABORTED
                && openStatementSeen) {
            throw new IOException("Sealed transaction contains an open statement");
        }
        return statements;
    }

    private static void validateWriters(
            Transaction transaction, Map<Long, StatementManifest> statements) throws IOException {
        Set<Long> writerIds = new HashSet<>();
        Set<String> writerRequests = new HashSet<>();
        for (WriterAssignment writer : transaction.getWritersList()) {
            if (writer.getWriterId() <= 0
                    || writer.getRequestId().isEmpty()
                    || writer.getRequestId().length() > MAX_IDENTITY_CHARACTERS
                    || !writerIds.add(writer.getWriterId())
                    || !writerRequests.add(writer.getRequestId())
                    || !statements.containsKey(writer.getStatementId())) {
                throw new IOException("Invalid writer assignment in checkpoint");
            }
        }
    }

    private static void validateStreamsAndSeals(
            Transaction transaction,
            Map<Long, StatementManifest> statements,
            Map<Long, TableSpec> tables)
            throws IOException {
        Set<StreamId> streams = new HashSet<>();
        for (StreamId stream : transaction.getStreamsList()) {
            StatementManifest statement = statements.get(stream.getStatementId());
            if (!streams.add(stream)
                    || stream.getTransactionId() != transaction.getTransactionId()
                    || stream.getKind() != MutationKind.APPEND_ROWS
                    || statement == null
                    || statement.getTableId() != stream.getTableId()) {
                throw new IOException("Invalid stream identity in checkpoint");
            }
            IngestWire.decode(stream);
            IngestWire.route(tables.get(stream.getTableId()), stream.getShardId());
        }

        List<StreamSeal> statementSeals = new ArrayList<>();
        for (StatementManifest statement : transaction.getStatementsList()) {
            statementSeals.addAll(statement.getSealsList());
        }
        List<StreamSeal> transactionSeals = canonical(transaction.getSealsList());
        if (!transactionSeals.equals(transaction.getSealsList())
                || !transactionSeals.equals(canonical(statementSeals))) {
            throw new IOException("Transaction and statement seals differ in checkpoint");
        }
        Set<StreamId> sealedStreams = new HashSet<>();
        for (StreamSeal seal : transactionSeals) {
            IngestWire.decode(seal);
            if (!sealedStreams.add(seal.getStream()) || !streams.contains(seal.getStream())) {
                throw new IOException("Invalid or duplicate stream seal in checkpoint");
            }
        }
        if (transaction.getState() != TransactionState.OPEN && !sealedStreams.equals(streams)) {
            throw new IOException("Sealed transaction manifest is incomplete");
        }
    }

    private static void validatePrepareTokens(Transaction transaction) throws IOException {
        Set<String> owners;
        try {
            owners = IngestWire.owners(transaction);
        }
        catch (IllegalArgumentException e) {
            throw new IOException("Invalid participant ownership in checkpoint", e);
        }
        Set<String> tokenOwners = new HashSet<>();
        for (PrepareToken token : transaction.getTokensList()) {
            if (!owners.contains(token.getOwner())
                    || !tokenOwners.add(token.getOwner())
                    || !token.getDigest().equals(ByteString.copyFrom(
                            IngestWire.prepareDigest(transaction, token.getOwner())))) {
                throw new IOException("Invalid participant Prepare token in checkpoint");
            }
        }
        boolean requiresTokens = transaction.getState() == TransactionState.PREPARED
                || IngestWire.committed(transaction)
                || transaction.getState() == TransactionState.ABORTED
                        && transaction.getTokensCount() > 0;
        if (requiresTokens && !tokenOwners.equals(owners)) {
            throw new IOException("Participant Prepare token coverage is incomplete");
        }
        if (!requiresTokens && !tokenOwners.isEmpty()) {
            throw new IOException("Unprepared transaction contains Prepare tokens");
        }
    }

    private static void validateDecisionState(
            Transaction transaction, CoordinatorSnapshot recovered) throws IOException {
        switch (transaction.getState()) {
            case OPEN:
            case SEALED:
            case PREPARED:
                if (transaction.getOutcome() != DecisionOutcome.UNDECIDED
                        || transaction.getProgress() != PublicationProgress.PRIVATE
                        || transaction.getCommitTimestamp() != 0
                        || !transaction.getCommitToken().isEmpty()) {
                    throw new IOException("Undecided transaction contains a durable outcome");
                }
                break;
            case COMMIT_DECIDED:
                validateCommittedTransaction(transaction, recovered);
                if (transaction.getProgress() != PublicationProgress.INSTALLING
                        || transaction.getCommitTimestamp() <= recovered.getPublishedTimestamp()) {
                    throw new IOException("Invalid pending publication state in checkpoint");
                }
                break;
            case PUBLISHED:
                validateCommittedTransaction(transaction, recovered);
                if (transaction.getProgress() != PublicationProgress.VISIBLE_NOW
                        || transaction.getCommitTimestamp() > recovered.getPublishedTimestamp()) {
                    throw new IOException("Invalid published transaction in checkpoint");
                }
                break;
            case ABORTED:
                if (transaction.getOutcome() != DecisionOutcome.ABORT
                        || transaction.getProgress() != PublicationProgress.PRIVATE
                        || transaction.getCommitTimestamp() != 0
                        || !transaction.getCommitToken().isEmpty()
                        || !transaction.getRollbackOnly()) {
                    throw new IOException("Invalid aborted transaction in checkpoint");
                }
                break;
            default:
                throw new IOException("Unknown transaction state in checkpoint");
        }
    }

    private static void validateCommittedTransaction(
            Transaction transaction, CoordinatorSnapshot recovered) throws IOException {
        if (transaction.getOutcome() != DecisionOutcome.COMMIT
                || transaction.getCommitTimestamp() <= 0
                || transaction.getCommitTimestamp() > recovered.getLastCommitTimestamp()
                || transaction.getCommitToken().isEmpty()
                || transaction.getRollbackOnly()) {
            throw new IOException("Invalid durable COMMIT state in checkpoint");
        }
    }

    private static void validateTerminalTransaction(
            Transaction transaction, CoordinatorSnapshot recovered) throws IOException {
        if (transaction.getStreamsCount() != 0
                || transaction.getSealsCount() != 0
                || transaction.getTokensCount() != 0
                || transaction.getWritersCount() != 0
                || (transaction.getState() != TransactionState.PUBLISHED
                        && transaction.getState() != TransactionState.ABORTED)) {
            throw new IOException("Invalid compact terminal transaction fence");
        }
        for (StatementManifest statement : transaction.getStatementsList()) {
            if (statement.getSealsCount() != 0
                    || transaction.getState() == TransactionState.PUBLISHED
                            && statement.getState() != StatementState.STATEMENT_COMPLETE) {
                throw new IOException("Invalid terminal statement fence");
            }
        }
        if (transaction.getState() == TransactionState.PUBLISHED) {
            validateCommittedTransaction(transaction, recovered);
            if (transaction.getProgress() != PublicationProgress.VISIBLE_NOW
                    || transaction.getCommitTimestamp() > recovered.getPublishedTimestamp()) {
                throw new IOException("Invalid published terminal transaction fence");
            }
        }
        else if (transaction.getOutcome() != DecisionOutcome.ABORT
                || transaction.getProgress() != PublicationProgress.PRIVATE
                || transaction.getCommitTimestamp() != 0
                || !transaction.getCommitToken().isEmpty()
                || !transaction.getRollbackOnly()) {
            throw new IOException("Invalid aborted terminal transaction fence");
        }
    }

    private int position(long id) throws IOException {
        checkOpen();
        for (int i = 0; i < snapshot.getTransactionsCount(); i++) {
            if (snapshot.getTransactions(i).getTransactionId() == id) {
                return i;
            }
        }
        throw new IOException("Active ingest transaction is absent: " + id);
    }

    private void replace(Transaction value) throws IOException {
        save(
                snapshot.toBuilder()
                        .setTransactions(position(value.getTransactionId()), value)
                        .build());
    }

    private void replaceDeferred(Transaction value) throws IOException {
        saveDeferred(
                snapshot.toBuilder()
                        .setTransactions(position(value.getTransactionId()), value)
                        .build());
    }

    private long expiry() {
        return Math.addExact(clock.millis(), leaseMillis);
    }

    private void live(Transaction tx) throws IOException {
        if (tx.getExpiresAtMillis() <= clock.millis()) {
            throw new IOException("Transaction lease expired: " + tx.getTransactionId());
        }
    }

    private Transaction transaction(long id) throws IOException {
        checkOpen();
        for (Transaction transaction : snapshot.getTransactionsList()) {
            if (transaction.getTransactionId() == id) {
                return transaction;
            }
        }
        for (TerminalTransaction terminal : snapshot.getTerminalTransactionsList()) {
            if (terminal.getTransaction().getTransactionId() == id) {
                return terminalTransaction(terminal);
            }
        }
        if (id <= snapshot.getRetiredTransactionIdHighWatermark()) {
            throw new IOException("Retired ingest transaction " + id
                    + "; terminal retry metadata is no longer retained");
        }
        throw new IOException("Unknown ingest transaction " + id + "; absence is not ABORT");
    }

    public synchronized Transaction get(long id) throws IOException {
        synchronizeState(stateSequence);
        return transaction(id);
    }

    public synchronized long publishedTimestamp() {
        long timestamp = snapshot.getPublishedTimestamp();
        try {
            checkOpen();
            synchronizeState(publicationSequence);
        }
        catch (IOException e) {
            throw new IllegalStateException("Cannot read durable published timestamp", e);
        }
        return timestamp;
    }

    public synchronized long lastCommitTimestamp() {
        try {
            synchronizeState(stateSequence);
        }
        catch (IOException e) {
            throw new IllegalStateException("Cannot read durable commit timestamp", e);
        }
        return snapshot.getLastCommitTimestamp();
    }

    private static void validateStatementIdentity(
            long statementId, String queryId, long ordinal, long frontier) {
        if (statementId <= 0 || queryId.isEmpty()
                || queryId.length() > MAX_IDENTITY_CHARACTERS
                || ordinal <= 0 || frontier < 0 || frontier >= ordinal) {
            throw new IllegalArgumentException("Invalid statement identity or private-read frontier");
        }
    }

    private static StatementManifest openStatement(
            long statementId, String queryId, long ordinal, long frontier, long tableId,
            boolean writeTable) {
        validateStatementIdentity(statementId, queryId, ordinal, frontier);
        StatementManifest.Builder statement = StatementManifest.newBuilder()
                .setStatementId(statementId)
                .setQueryId(queryId)
                .setOrdinal(ordinal)
                .setReadOwnThroughOrdinal(frontier)
                .addReadTableIds(tableId)
                .setState(StatementState.STATEMENT_OPEN);
        if (writeTable) {
            statement.setTableId(tableId);
        }
        return statement.build();
    }

    public Transaction begin(BeginWriteRequest request) throws Exception {
        if (request.getRequestId().isEmpty()
                || request.getRequestId().length() > MAX_IDENTITY_CHARACTERS) {
            throw new IllegalArgumentException("A bounded request id is required");
        }
        synchronized (this) {
            checkOpen();
            for (Transaction existing : snapshot.getTransactionsList()) {
                if (existing.getRequestId().equals(request.getRequestId())) {
                    validateBeginRetry(existing, request);
                    synchronizeState(stateSequence);
                    return existing;
                }
            }
            for (TerminalTransaction terminal : snapshot.getTerminalTransactionsList()) {
                if (terminal.getTransaction().getRequestId().equals(request.getRequestId())) {
                    Transaction existing = terminalTransaction(terminal);
                    validateBeginRetry(existing, request);
                    synchronizeState(stateSequence);
                    return existing;
                }
            }
            if (snapshot.getTransactionsCount() >= maxTransactions) {
                throw new IOException("Transaction metadata capacity reached");
            }
        }
        TableSpec descriptor = tables.load(request.getSchemaName(), request.getTableName());
        synchronized (this) {
            // Concurrent retransmissions must not allocate two transaction identities.
            for (Transaction existing : snapshot.getTransactionsList()) {
                if (existing.getRequestId().equals(request.getRequestId())) {
                    validateBeginRetry(existing, request);
                    synchronizeState(stateSequence);
                    return existing;
                }
            }
            for (TerminalTransaction terminal : snapshot.getTerminalTransactionsList()) {
                if (terminal.getTransaction().getRequestId().equals(request.getRequestId())) {
                    Transaction existing = terminalTransaction(terminal);
                    validateBeginRetry(existing, request);
                    synchronizeState(stateSequence);
                    return existing;
                }
            }
            if (request.getReadTimestamp() > snapshot.getPublishedTimestamp()) {
                throw new IOException("Unpublished read timestamp");
            }
            pinRoutes(descriptor.getRoutesList());
            if (request.getStatementOrdinal() != FIRST_STATEMENT_ORDINAL) {
                throw new IOException("The first statement ordinal must be one");
            }
            validateStatementIdentity(request.getStatementId(), request.getQueryId(),
                    request.getStatementOrdinal(), EMPTY_PRIVATE_READ_FRONTIER);
            Transaction tx =
                    Transaction.newBuilder()
                            .setTransactionId(ids.getAsLong())
                            .setRequestId(request.getRequestId())
                            .setReadTimestamp(request.getReadTimestamp())
                            .setTable(descriptor)
                            .setState(TransactionState.OPEN)
                            .setExpiresAtMillis(expiry())
                            .build();
            tx = tx.toBuilder()
                        .addEnlistedTables(descriptor)
                        .addStatements(openStatement(request.getStatementId(),
                                request.getQueryId(), request.getStatementOrdinal(),
                                EMPTY_PRIVATE_READ_FRONTIER, descriptor.getTableId(), true))
                        .setScope(request.getScope())
                        .setRepresentation(request.getRepresentation())
                        .setAckMode(request.getAckMode())
                        .setOutcome(DecisionOutcome.UNDECIDED)
                        .setProgress(PublicationProgress.PRIVATE)
                        .build();
            for (Transaction old : snapshot.getTransactionsList()) {
                if (old.getTransactionId() == tx.getTransactionId()) {
                    throw new IOException("Allocator reused transaction identity");
                }
            }
            if (tx.getTransactionId() <= snapshot.getRetiredTransactionIdHighWatermark()) {
                throw new IOException("Allocator reused a retired transaction identity");
            }
            if (snapshot.getTransactionsCount() >= maxTransactions) {
                throw new IOException("Transaction metadata capacity reached");
            }
            save(snapshot.toBuilder().addTransactions(tx).build());
            return tx;
        }
    }

    private static void validateBeginRetry(Transaction existing, BeginWriteRequest request)
            throws IOException {
        if (!existing.getTable().getSchemaName().equals(request.getSchemaName())
                || !existing.getTable().getTableName().equals(request.getTableName())
                || existing.getReadTimestamp() != request.getReadTimestamp()
                || existing.getScope() != request.getScope()
                || existing.getRepresentation() != request.getRepresentation()
                || existing.getAckMode() != request.getAckMode()
                || existing.getStatementsCount() == 0
                || existing.getStatements(0).getStatementId() != request.getStatementId()
                || !existing.getStatements(0).getQueryId().equals(request.getQueryId())
                || existing.getStatements(0).getOrdinal() != request.getStatementOrdinal()) {
            throw new IOException("Begin request identity reused with different arguments");
        }
    }

    public Transaction beginStatement(BeginStatementRequest request) throws Exception {
        validateStatementIdentity(request.getStatementId(), request.getQueryId(),
                request.getOrdinal(), request.getReadOwnThroughOrdinal());
        synchronized (this) {
            Transaction tx = transaction(request.getTransactionId());
            boolean existingStatement = false;
            for (StatementManifest statement : tx.getStatementsList()) {
                if (statement.getStatementId() == request.getStatementId()) {
                    if (!statement.getQueryId().equals(request.getQueryId())
                            || statement.getOrdinal() != request.getOrdinal()
                            || statement.getReadOwnThroughOrdinal()
                                != request.getReadOwnThroughOrdinal()) {
                        throw new IOException("Statement identity reused with different arguments");
                    }
                    if (statement.getState() != StatementState.STATEMENT_OPEN) {
                        throw new IOException("Completed statement cannot enlist another table");
                    }
                    existingStatement = true;
                }
            }
            if (tx.getState() != TransactionState.OPEN || tx.getRollbackOnly()) {
                throw new IOException("Transaction no longer accepts statements");
            }
            live(tx);
            if (!existingStatement) {
                long completed = 0;
                for (StatementManifest statement : tx.getStatementsList()) {
                    if (statement.getState() != StatementState.STATEMENT_COMPLETE) {
                        throw new IOException("Overlapping statements are not supported");
                    }
                    completed = Math.max(completed, statement.getOrdinal());
                }
                if (request.getOrdinal() != completed + FIRST_STATEMENT_ORDINAL
                        || request.getReadOwnThroughOrdinal() != completed) {
                    throw new IOException(
                            "Statement ordinal or private-read frontier is not contiguous");
                }
            }
        }
        TableSpec descriptor = tables.load(request.getSchemaName(), request.getTableName());
        synchronized (this) {
            Transaction tx = transaction(request.getTransactionId());
            int existingPosition = -1;
            StatementManifest existingStatement = null;
            for (int index = 0; index < tx.getStatementsCount(); index++) {
                StatementManifest statement = tx.getStatements(index);
                if (statement.getStatementId() == request.getStatementId()) {
                    if (!statement.getQueryId().equals(request.getQueryId())
                            || statement.getOrdinal() != request.getOrdinal()
                            || statement.getReadOwnThroughOrdinal()
                                != request.getReadOwnThroughOrdinal()) {
                        throw new IOException(
                                "Statement identity reused with different arguments");
                    }
                    existingPosition = index;
                    existingStatement = statement;
                    break;
                }
            }
            if (tx.getState() != TransactionState.OPEN || tx.getRollbackOnly()) {
                throw new IOException("Transaction no longer accepts statements");
            }
            if (existingStatement == null) {
                long completed = tx.getStatementsList().stream()
                        .filter(s -> s.getState() == StatementState.STATEMENT_COMPLETE)
                        .mapToLong(StatementManifest::getOrdinal).max()
                        .orElse(EMPTY_PRIVATE_READ_FRONTIER);
                if (request.getOrdinal() != completed + FIRST_STATEMENT_ORDINAL
                        || request.getReadOwnThroughOrdinal() != completed) {
                    throw new IOException("Statement frontier changed while enlisting table");
                }
            }
            else if (existingStatement.getState() != StatementState.STATEMENT_OPEN) {
                throw new IOException("Completed statement cannot enlist another table");
            }
            pinRoutes(descriptor.getRoutesList());
            Transaction.Builder next = tx.toBuilder();
            boolean enlisted = false;
            for (TableSpec table : tx.getEnlistedTablesList()) {
                if (table.getTableId() == descriptor.getTableId()) {
                    if (!table.equals(descriptor)) {
                        throw new IOException("Enlisted table metadata changed within transaction");
                    }
                    enlisted = true;
                }
            }
            if (!enlisted) {
                next.addEnlistedTables(descriptor);
            }
            if (existingStatement == null) {
                next.addStatements(openStatement(request.getStatementId(), request.getQueryId(),
                        request.getOrdinal(), request.getReadOwnThroughOrdinal(),
                        descriptor.getTableId(), request.getWriteTable()));
            }
            else {
                StatementManifest.Builder statement = existingStatement.toBuilder();
                if (!existingStatement.getReadTableIdsList().contains(descriptor.getTableId())) {
                    statement.addReadTableIds(descriptor.getTableId());
                }
                if (request.getWriteTable()) {
                    if (existingStatement.getTableId() != 0
                            && existingStatement.getTableId() != descriptor.getTableId()) {
                        throw new IOException("Statement already has a different write table");
                    }
                    statement.setTableId(descriptor.getTableId());
                }
                next.setStatements(existingPosition, statement);
            }
            Transaction updated = next.setExpiresAtMillis(expiry()).build();
            replace(updated);
            return updated;
        }
    }

    private void pinRoutes(List<Route> proposed) throws IOException {
        if (proposed.isEmpty()) {
            throw new IOException("No Retina route available");
        }
        Set<Integer> ids = new HashSet<>();
        for (Route route : proposed) {
            if (!ids.add(route.getShardId())
                    || route.getHost().isEmpty()
                    || route.getPort() <= 0
                    || route.getPort() > 65535) {
                throw new IOException("Invalid or duplicate shard route");
            }
        }
        if (snapshot.getRoutesCount() == 0) {
            save(snapshot.toBuilder().addAllRoutes(proposed).build());
        } else if (!snapshot.getRoutesList().equals(proposed)) {
            throw new IOException(
                    "LOCAL ingest topology changed; recover the original owners and volumes before"
                        + " proceeding");
        }
    }

    public Publication publication() throws Exception {
        List<Route> current = tables.routes();
        synchronized (this) {
            checkOpen();
            pinRoutes(current);
            Publication publication = Publication.newBuilder()
                    .setPublishedTimestamp(snapshot.getPublishedTimestamp())
                    .addAllRoutes(snapshot.getRoutesList())
                    .build();
            // Capture the read boundary before synchronization releases the monitor.
            // Unrelated deferred write state does not advance this boundary.
            synchronizeState(publicationSequence);
            return publication;
        }
    }

    /** Allocate an idempotent, transaction-local identity for one physical writer. */
    public synchronized WriterAssignment allocateWriter(AllocateWriterRequest request)
            throws IOException {
        if (request.getRequestId().isEmpty()
                || request.getRequestId().length() > MAX_IDENTITY_CHARACTERS) {
            throw new IllegalArgumentException("A bounded writer request id is required");
        }
        Transaction tx = transaction(request.getTransactionId());
        if (tx.getState() == TransactionState.ABORTED) {
            throw new IOException("Transaction aborted");
        }
        long maximum = 0;
        for (WriterAssignment writer : tx.getWritersList()) {
            if (writer.getRequestId().equals(request.getRequestId())) {
                if (writer.getTaskId() != request.getTaskId()
                        || writer.getStatementId() != request.getStatementId()) {
                    throw new IOException("Writer request reused with a different task identity");
                }
                synchronizeState(stateSequence);
                return writer;
            }
            maximum = Math.max(maximum, writer.getWriterId());
        }
        if (tx.getState() != TransactionState.OPEN) {
            throw new IOException("Transaction input is sealed");
        }
        live(tx);
        boolean openStatement = false;
        for (StatementManifest statement : tx.getStatementsList()) {
            if (statement.getStatementId() == request.getStatementId()
                    && statement.getState() == StatementState.STATEMENT_OPEN) {
                openStatement = true;
            }
        }
        if (!openStatement) {
            throw new IOException("Writer does not belong to the open statement");
        }
        if (tx.getWritersCount() >= maxStreams) {
            throw new IOException("Transaction writer limit exceeded");
        }
        // Existing clients may already have registered a stream before using this API.
        for (StreamId stream : tx.getStreamsList()) {
            maximum = Math.max(maximum, stream.getWriterId());
        }
        if (maximum == Long.MAX_VALUE) {
            throw new IOException("Transaction writer identities exhausted");
        }
        WriterAssignment assignment =
                WriterAssignment.newBuilder()
                        .setRequestId(request.getRequestId())
                        .setTaskId(request.getTaskId())
                        .setWriterId(maximum + 1)
                        .setStatementId(request.getStatementId())
                        .build();
        replace(tx.toBuilder().addWriters(assignment).setExpiresAtMillis(expiry()).build());
        return assignment;
    }

    public synchronized Transaction register(StreamId stream) throws IOException {
        Transaction tx = transaction(stream.getTransactionId());
        IngestWire.decode(stream);
        if (stream.getKind() != MutationKind.APPEND_ROWS) {
            throw new IOException("Stream does not match the INSERT transaction");
        }
        TableSpec table = IngestWire.table(tx, stream.getTableId());
        IngestWire.route(table, stream.getShardId());
        boolean openStatement = false;
        for (StatementManifest statement : tx.getStatementsList()) {
            if (statement.getStatementId() == stream.getStatementId()
                    && statement.getTableId() == stream.getTableId()
                    && statement.getState() == StatementState.STATEMENT_OPEN) {
                openStatement = true;
            }
        }
        if (!openStatement) {
            throw new IOException("Stream does not belong to the open statement");
        }
        if (tx.getStreamsList().contains(stream)) {
            if (tx.getState() == TransactionState.ABORTED) {
                throw new IOException("Transaction aborted");
            }
            synchronizeState(stateSequence);
            return tx;
        }
        if (tx.getState() != TransactionState.OPEN) {
            throw new IOException("Transaction input is sealed");
        }
        live(tx);
        if (tx.getStreamsCount() >= maxStreams) {
            throw new IOException("Transaction stream limit exceeded");
        }
        tx = tx.toBuilder().addStreams(stream).setExpiresAtMillis(expiry()).build();
        // Registration is replayable admission state. Statement completion supplies the
        // durability barrier for its exact stream manifest.
        replaceDeferred(tx);
        return tx;
    }

    public synchronized Transaction touch(long id) throws IOException {
        Transaction tx = transaction(id);
        if (tx.getState() == TransactionState.ABORTED || IngestWire.committed(tx)) {
            synchronizeState(stateSequence);
            return tx;
        }
        live(tx);
        tx = tx.toBuilder().setExpiresAtMillis(expiry()).build();
        replace(tx);
        return tx;
    }

    private static List<StreamSeal> canonical(List<StreamSeal> input) {
        List<StreamSeal> result = new ArrayList<>(input);
        result.sort(
                Comparator.comparingLong((StreamSeal s) -> s.getStream().getStatementId())
                        .thenComparingLong(s -> s.getStream().getTableId())
                        .thenComparingLong(s -> s.getStream().getWriterId())
                        .thenComparingInt(s -> s.getStream().getShardId())
                        .thenComparingInt(s -> s.getStream().getKindValue()));
        return result;
    }

    public synchronized Transaction completeStatement(CompleteStatementRequest request)
            throws IOException {
        Transaction tx = transaction(request.getTransactionId());
        if (tx.getState() != TransactionState.OPEN || tx.getRollbackOnly()) {
            throw new IOException("Transaction no longer accepts statement completion");
        }
        live(tx);
        int statementPosition = -1;
        StatementManifest statement = null;
        for (int i = 0; i < tx.getStatementsCount(); i++) {
            if (tx.getStatements(i).getStatementId() == request.getStatementId()) {
                statementPosition = i;
                statement = tx.getStatements(i);
                break;
            }
        }
        if (statement == null) {
            throw new IOException("Unknown statement identity");
        }
        List<StreamSeal> seals = canonical(request.getSealsList());
        if (statement.getState() == StatementState.STATEMENT_COMPLETE) {
            if (!statement.getSealsList().equals(seals)) {
                throw new IOException("Completed statement manifest mismatch");
            }
            synchronizeState(stateSequence);
            return tx;
        }
        Set<StreamId> expected = new HashSet<>();
        for (StreamId stream : tx.getStreamsList()) {
            if (stream.getStatementId() == request.getStatementId()) {
                expected.add(stream);
            }
        }
        Set<StreamId> actual = new HashSet<>();
        for (StreamSeal seal : seals) {
            IngestWire.decode(seal);
            if (!actual.add(seal.getStream())
                    || seal.getStream().getTransactionId() != tx.getTransactionId()
                    || seal.getStream().getStatementId() != request.getStatementId()) {
                throw new IOException("Duplicate or foreign statement stream");
            }
        }
        if (!actual.equals(expected)) {
            throw new IOException("Statement manifest must exactly cover its registered streams");
        }
        StatementManifest complete = statement.toBuilder()
                .clearSeals()
                .addAllSeals(seals)
                .setState(StatementState.STATEMENT_COMPLETE)
                .build();
        complete = complete.toBuilder()
                .setDigest(ByteString.copyFrom(IngestWire.statementDigest(complete)))
                .build();
        Transaction updated = tx.toBuilder()
                .setStatements(statementPosition, complete)
                .addAllSeals(seals)
                .setExpiresAtMillis(expiry())
                .build();
        replace(updated);
        return updated;
    }

    public Transaction prepare(PrepareWriteRequest request) throws Exception {
        Transaction tx;
        List<StreamSeal> seals = canonical(request.getSealsList());
        synchronized (this) {
            tx = transaction(request.getTransactionId());
            List<StreamSeal> completed = canonical(tx.getSealsList());
            if (!seals.isEmpty() && !seals.equals(completed)) {
                throw new IOException("Transaction manifest differs from completed statements");
            }
            seals = completed;
            for (StatementManifest statement : tx.getStatementsList()) {
                if (statement.getState() != StatementState.STATEMENT_COMPLETE
                        || !statement.getDigest().equals(ByteString.copyFrom(
                            IngestWire.statementDigest(statement)))) {
                    throw new IOException("Prepare requires every exact statement manifest");
                }
            }
            if (tx.getState() == TransactionState.ABORTED) {
                throw new IOException("Transaction aborted");
            }
            if (IngestWire.committed(tx)) {
                synchronizeState(stateSequence);
                return tx;
            }
            if (tx.getState() != TransactionState.OPEN) {
                if (!tx.getSealsList().equals(seals)) {
                    throw new IOException("Sealed manifest mismatch");
                }
                if (tx.getState() == TransactionState.PREPARED || IngestWire.committed(tx)) {
                    if (IngestWire.committed(tx)) {
                        synchronizeState(stateSequence);
                    }
                    return tx;
                }
            } else {
                live(tx);
                Set<StreamId> declared = new HashSet<>();
                for (StreamSeal seal : seals) {
                    IngestWire.decode(seal);
                    if (!declared.add(seal.getStream())
                            || seal.getStream().getTransactionId() != tx.getTransactionId()) {
                        throw new IOException("Duplicate or foreign manifest stream");
                    }
                }
                if (!declared.equals(new HashSet<>(tx.getStreamsList()))) {
                    throw new IOException("Manifest must exactly cover every registered stream");
                }
                tx =
                        tx.toBuilder()
                                .clearSeals()
                                .addAllSeals(seals)
                                .setState(TransactionState.SEALED)
                                .setExpiresAtMillis(expiry())
                                .build();
                // The participant Prepare and the later decision are the durable boundaries.
                replaceDeferred(tx);
            }
        }
        List<PrepareToken> tokens = new ArrayList<>();
        for (String owner : IngestWire.owners(tx)) {
            PrepareToken token = participants.prepare(owner, tx);
            if (!token.getOwner().equals(owner)
                    || !token.getDigest()
                            .equals(ByteString.copyFrom(IngestWire.prepareDigest(tx, owner)))) {
                throw new IOException("Prepare token mismatch from participant");
            }
            tokens.add(token);
        }
        synchronized (this) {
            Transaction current = transaction(tx.getTransactionId());
            if (current.getState() == TransactionState.ABORTED) {
                throw new IOException("Transaction aborted during Prepare");
            }
            if (current.getState() == TransactionState.PREPARED || IngestWire.committed(current)) {
                if (IngestWire.committed(current)) {
                    synchronizeState(stateSequence);
                }
                return current;
            }
            live(current);
            tx =
                    current.toBuilder()
                            .clearTokens()
                            .addAllTokens(tokens)
                            .setState(TransactionState.PREPARED)
                            .setExpiresAtMillis(expiry())
                            .build();
            // COMMIT synchronizes this token record and its decision in one WAL prefix.
            replaceDeferred(tx);
            return tx;
        }
    }

    public Transaction commit(long id) throws Exception {
        Transaction decided;
        synchronized (this) {
            Transaction tx = transaction(id);
            if (tx.getState() == TransactionState.PUBLISHED) {
                synchronizeState(stateSequence);
                return tx;
            }
            if (tx.getState() == TransactionState.ABORTED) {
                throw new IOException("Transaction is ABORTED");
            }
            if (!IngestWire.committed(tx)) {
                if (tx.getState() != TransactionState.PREPARED) {
                    throw new IOException("Commit requires PREPARED");
                }
                live(tx);
                long timestamp = ids.getAsLong();
                if (timestamp <= snapshot.getLastCommitTimestamp()
                        || timestamp <= 0
                        || timestamp >= MAX_RETINA_TIMESTAMP) {
                    throw new IOException(
                            "Commit allocator is not monotonic or exceeds Retina timestamp width");
                }
                decided =
                        tx.toBuilder()
                                .setState(TransactionState.COMMIT_DECIDED)
                                .setCommitTimestamp(timestamp)
                                .setOutcome(DecisionOutcome.COMMIT)
                                .setProgress(PublicationProgress.INSTALLING)
                                .setCommitToken("pixels-ingest:" + id + ":" + timestamp + ":"
                                        + UUID.randomUUID())
                                .build();
                save(
                        snapshot.toBuilder()
                                .setTransactions(position(id), decided)
                                .setLastCommitTimestamp(timestamp)
                                .build());
            }
            else {
                // Another caller may still be synchronizing this decision.
                synchronizeState(stateSequence);
                decided = tx;
            }
        }
        if (decided.getAckMode() == CommitAckMode.VISIBLE) {
            forceFileThrough(decided.getCommitTimestamp());
            drivePublicationThrough(decided.getCommitTimestamp());
            return get(id);
        }
        // The decision above is durable. Do not make its acknowledgement wait for
        // unrelated transactions appended after that durability boundary.
        return decided;
    }

    public void drivePublication() throws Exception {
        drivePublicationThrough(Long.MAX_VALUE);
    }

    private void drivePublicationThrough(long boundary) throws Exception {
        drivePublicationThrough(boundary, Long.MAX_VALUE);
    }

    private void drivePublicationThrough(long boundary, long deadlineMillis) throws Exception {
        synchronized (publisher) {
            while (!closed) {
                if (clock.millis() >= deadlineMillis) {
                    throw new IOException(
                            "Visibility deadline expired while publication remains durable");
                }
                collectCompletedInstallations();
                Transaction next;
                Future<Boolean> installation;
                boolean ready;
                synchronized (this) {
                    if (snapshot.getPublishedTimestamp() >= boundary) {
                        return;
                    }
                    List<Transaction> pending = new ArrayList<>();
                    for (Transaction transaction : snapshot.getTransactionsList()) {
                        if (transaction.getState() == TransactionState.COMMIT_DECIDED) {
                            pending.add(transaction);
                        }
                    }
                    pending.sort(Comparator.comparingLong(Transaction::getCommitTimestamp));
                    next = pending.isEmpty() ? null : pending.get(0);
                    boolean mayForceTail = forceFileThroughTimestamp > snapshot.getPublishedTimestamp();
                    if (mayForceTail) {
                        for (Transaction transaction : pending) {
                            if (transaction.getCommitTimestamp() <= forceFileThroughTimestamp
                                    && !installationContributed.contains(
                                            transaction.getTransactionId())) {
                                mayForceTail = false;
                                break;
                            }
                        }
                    }
                    for (Transaction transaction : pending) {
                        long transactionId = transaction.getTransactionId();
                        if (installationReady.contains(transactionId)
                                || installationTasks.containsKey(transactionId)) {
                            continue;
                        }
                        boolean contributed = installationContributed.contains(transactionId);
                        boolean pollPublicationHead = transaction == next && contributed;
                        if (!contributed || pollPublicationHead) {
                            boolean forceFileTail = pollPublicationHead && mayForceTail;
                            installationTasks.put(
                                    transactionId,
                                    installationExecutor.submit(() -> {
                                        boolean installationComplete = true;
                                        for (String owner : IngestWire.owners(transaction)) {
                                            installationComplete &= participants.install(
                                                    owner, transaction, forceFileTail);
                                        }
                                        return installationComplete;
                                    }));
                        }
                    }
                    installation = next == null
                            ? null
                            : installationTasks.get(next.getTransactionId());
                    ready = next != null
                            && installationReady.contains(next.getTransactionId());
                }
                if (next == null) {
                    return;
                }
                if (!ready) {
                    if (installation == null) {
                        Thread.sleep(MAX_VISIBILITY_POLL_MILLIS);
                        continue;
                    }
                    try {
                        installation.get(MAX_VISIBILITY_POLL_MILLIS, TimeUnit.MILLISECONDS);
                    }
                    catch (TimeoutException ignored) {
                        // Refresh the pending set so later committed transactions can contribute
                        // to the same file group while the publication-prefix transaction waits.
                    }
                    catch (ExecutionException e) {
                        discardFailedInstallation(next.getTransactionId(), installation);
                        throw installationFailure(e);
                    }
                    continue;
                }
                synchronized (this) {
                    Transaction current = transaction(next.getTransactionId());
                    if (current.getState() != TransactionState.COMMIT_DECIDED) {
                        throw new IOException("Invalid installation transition");
                    }
                    save(
                            snapshot.toBuilder()
                                    .setTransactions(
                                            position(current.getTransactionId()),
                                            current.toBuilder()
                                                    .setState(TransactionState.PUBLISHED)
                                                    .setOutcome(DecisionOutcome.COMMIT)
                                                    .setProgress(PublicationProgress.VISIBLE_NOW))
                                    .setPublishedTimestamp(current.getCommitTimestamp())
                                    .build());
                    installationTasks.remove(current.getTransactionId(), installation);
                    installationContributed.remove(current.getTransactionId());
                    installationReady.remove(current.getTransactionId());
                }
                // Release reservations only after the common snapshot is published.
                for (String owner : IngestWire.owners(next)) {
                    try {
                        participants.discard(owner, next.getTransactionId());
                    } catch (Exception e) {
                        LOG.warn("Reservation release will retry: {}", e.toString());
                    }
                }
            }
        }
    }

    private void collectCompletedInstallations() throws Exception {
        List<Map.Entry<Long, Future<Boolean>>> completed = new ArrayList<>();
        synchronized (this) {
            for (Map.Entry<Long, Future<Boolean>> entry : installationTasks.entrySet()) {
                if (entry.getValue().isDone()) {
                    completed.add(entry);
                }
            }
        }
        for (Map.Entry<Long, Future<Boolean>> entry : completed) {
            boolean ready;
            try {
                ready = entry.getValue().get();
            }
            catch (ExecutionException e) {
                discardFailedInstallation(entry.getKey(), entry.getValue());
                throw installationFailure(e);
            }
            synchronized (this) {
                if (installationTasks.remove(entry.getKey(), entry.getValue())) {
                    installationContributed.add(entry.getKey());
                    if (ready) {
                        installationReady.add(entry.getKey());
                    }
                }
            }
        }
    }

    private synchronized void discardFailedInstallation(
            long transactionId, Future<Boolean> installation) {
        installationTasks.remove(transactionId, installation);
    }

    private static Exception installationFailure(ExecutionException failure) {
        Throwable cause = failure.getCause();
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return new IOException("Transaction installation failed", cause);
    }

    public Transaction awaitVisible(VisibilityRequest request) throws Exception {
        Transaction tx = get(request.getTransactionId());
        if (!IngestWire.committed(tx)
                || request.getCommitToken().isEmpty()
                || !request.getCommitToken().equals(tx.getCommitToken())) {
            throw new IOException("A valid durable COMMIT token is required");
        }
        long deadline = request.getDeadlineMillis();
        if (deadline <= clock.millis()) {
            throw new IOException("Visibility deadline expired for committed transaction "
                    + tx.getTransactionId());
        }
        forceFileThrough(tx.getCommitTimestamp());
        while (tx.getState() != TransactionState.PUBLISHED) {
            drivePublicationThrough(tx.getCommitTimestamp(), deadline);
            tx = get(tx.getTransactionId());
            if (tx.getState() == TransactionState.PUBLISHED) {
                break;
            }
            if (clock.millis() >= deadline) {
                throw new IOException("Visibility deadline expired; COMMIT remains durable for txId="
                        + tx.getTransactionId());
            }
            Thread.sleep(Math.min(MAX_VISIBILITY_POLL_MILLIS,
                    Math.max(MIN_VISIBILITY_POLL_MILLIS, deadline - clock.millis())));
        }
        return tx;
    }

    public VisibleBarrier flushVisibleBarrier(VisibleBarrierRequest request) throws Exception {
        long boundary;
        synchronized (this) {
            checkOpen();
            synchronizeState(stateSequence);
            boundary = snapshot.getLastCommitTimestamp();
        }
        if (request.getDeadlineMillis() <= clock.millis()) {
            throw new IOException("Visibility barrier deadline expired");
        }
        forceFileThrough(boundary);
        drivePublicationThrough(boundary, request.getDeadlineMillis());
        synchronized (this) {
            if (snapshot.getPublishedTimestamp() < boundary) {
                throw new IOException("Visibility barrier did not reach its committed boundary");
            }
            return VisibleBarrier.newBuilder().setCommittedBoundary(boundary)
                    .setPublishedTimestamp(snapshot.getPublishedTimestamp()).build();
        }
    }

    private synchronized void forceFileThrough(long boundary) {
        forceFileThroughTimestamp = Math.max(forceFileThroughTimestamp, boundary);
    }

    public Transaction abort(long id) throws Exception {
        Transaction tx;
        synchronized (this) {
            tx = transaction(id);
            if (IngestWire.committed(tx)) {
                synchronizeState(stateSequence);
                return tx;
            }
            if (tx.getState() != TransactionState.ABORTED) {
                tx = tx.toBuilder().setState(TransactionState.ABORTED)
                        .setOutcome(DecisionOutcome.ABORT)
                        .setProgress(PublicationProgress.PRIVATE)
                        .setRollbackOnly(true).build();
                replace(tx);
            }
        }
        // Cleanup is driven by reconciliation. Do not block a durable ABORT on an unavailable
        // participant.
        return tx;
    }

    public synchronized TransactionList list(String owner) throws IOException {
        synchronizeState(stateSequence);
        TransactionList.Builder result =
                TransactionList.newBuilder()
                        .setPublishedTimestamp(snapshot.getPublishedTimestamp());
        for (Transaction tx : snapshot.getTransactionsList()) {
            if (IngestWire.owners(tx).contains(owner)) {
                result.addTransactions(tx);
            }
        }
        return result.build();
    }

    public void expire() throws Exception {
        List<Long> expired = new ArrayList<>();
        synchronized (this) {
            for (Transaction tx : snapshot.getTransactionsList()) {
                if (!IngestWire.committed(tx)
                        && tx.getState() != TransactionState.ABORTED
                        && tx.getExpiresAtMillis() <= clock.millis()) {
                    expired.add(tx.getTransactionId());
                }
            }
        }
        for (long id : expired) {
            abort(id);
        }
    }

    private void cleanupTerminalReservations() throws Exception {
        List<Transaction> terminal;
        synchronized (this) {
            terminal = new ArrayList<>(snapshot.getTransactionsList());
        }
        for (Transaction tx : terminal) {
            if (tx.getState() == TransactionState.ABORTED
                    || tx.getState() == TransactionState.PUBLISHED) {
                for (String owner : IngestWire.owners(tx)) {
                    if (tx.getState() == TransactionState.PUBLISHED) {
                        participants.checkpoint(owner, tx.getTransactionId());
                    }
                    participants.discard(owner, tx.getTransactionId());
                }
                synchronized (this) {
                    int active = findActive(tx.getTransactionId());
                    if (active >= 0 && snapshot.getTransactions(active).getState() == tx.getState()) {
                        retire(active, tx);
                    }
                }
            }
        }
    }

    private int findActive(long transactionId) {
        for (int i = 0; i < snapshot.getTransactionsCount(); i++) {
            if (snapshot.getTransactions(i).getTransactionId() == transactionId) {
                return i;
            }
        }
        return -1;
    }

    private static Transaction terminalTransaction(TerminalTransaction terminal) {
        return terminal.getTransaction();
    }

    private void retire(int active, Transaction tx) throws IOException {
        Transaction.Builder result = tx.toBuilder()
                .clearStreams()
                .clearSeals()
                .clearTokens()
                .clearWriters();
        for (int index = 0; index < result.getStatementsCount(); index++) {
            result.setStatements(index, result.getStatements(index).toBuilder().clearSeals());
        }
        TerminalTransaction terminal = TerminalTransaction.newBuilder()
                .setTransaction(result)
                .setRetiredAtMillis(Math.max(
                        MINIMUM_RETIREMENT_TIMESTAMP_MILLIS, clock.millis()))
                .build();
        CoordinatorSnapshot.Builder next = snapshot.toBuilder()
                .removeTransactions(active)
                .addTerminalTransactions(terminal)
                .setRetiredTransactionIdHighWatermark(Math.max(
                        snapshot.getRetiredTransactionIdHighWatermark(), tx.getTransactionId()));
        compactTerminalFences(next);
        save(next.build());
    }

    private synchronized void pruneTerminalFences() throws IOException {
        CoordinatorSnapshot.Builder next = snapshot.toBuilder();
        if (compactTerminalFences(next)) {
            save(next.build());
        }
    }

    private boolean compactTerminalFences(CoordinatorSnapshot.Builder next) {
        List<TerminalTransaction> retained = new ArrayList<>(next.getTerminalTransactionsList());
        retained.sort(Comparator.comparingLong(TerminalTransaction::getRetiredAtMillis)
                .thenComparingLong(value -> value.getTransaction().getTransactionId()));
        long cutoff = clock.millis() - terminalRetentionMillis;
        int originalSize = retained.size();
        while (!retained.isEmpty()
                && (retained.size() > maxTerminalTransactions
                        || retained.get(0).getRetiredAtMillis() <= cutoff)) {
            retained.remove(0);
        }
        if (retained.size() == originalSize) {
            return false;
        }
        next.clearTerminalTransactions().addAllTerminalTransactions(retained);
        return true;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        recovery.shutdownNow();
        installationExecutor.shutdownNow();
        synchronized (publisher) {
            synchronized (this) {
                IOException failure = null;
                try {
                    synchronizeState(stateSequence);
                }
                catch (IOException e) {
                    failure = e;
                }
                try {
                    store.close();
                }
                catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    }
                    else {
                        failure.addSuppressed(e);
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            }
        }
    }
}
