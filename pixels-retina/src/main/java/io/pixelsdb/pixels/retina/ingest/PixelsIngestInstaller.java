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

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.service.*;
import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.common.ingest.wire.*;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.File;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.core.ingest.*;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.pixelsdb.pixels.retina.*;

import java.io.IOException;
import java.util.*;

/**
 * Installs into existing PixelsWriteBuffer and IndexService. The local plan file
 * records allocator results and replay placements; it is not a row-location index.
 * WAL payload and batch plans remain recovery-owned until a durable transaction
 * checkpoint covers data, indexes, visibility, and file identities.
 */
public final class PixelsIngestInstaller implements RetinaIngestParticipant.Installer {
    private static final long BASE_PLAN_RESERVATION_BYTES = 4096L;
    private static final long BATCH_PLAN_RESERVATION_BYTES = 2048L;
    private static final long VISIBILITY_WORD_BITS = 64L;
    private static final long VISIBILITY_WORD_RESERVATION_BYTES = 512L;
    private static final long VISIBILITY_PADDING_WORDS = 2L;
    private static final long KEY_INTENT_BASE_BYTES = 256L;
    private static final long KEY_INTENT_BYTE_MULTIPLIER = 2L;

    private final InstallationStateStore state;
    private final IngestOptions options;
    private final RetinaResourceManager resources;
    private final IndexService indexes;
    private final MetadataService metadata;
    private final String owner;
    private final Map<String, BatchInstall> plans = new LinkedHashMap<>();
    private final Set<String> installedBatches = new HashSet<>();
    private final Map<Long, TransactionCheckpoint> checkpoints = new LinkedHashMap<>();
    private final Map<String, Long> keyIntents = new HashMap<>();
    private final Map<Long, Set<String>> transactionKeys = new HashMap<>();
    private final Map<Long, Long> reservedBytes = new HashMap<>();
    private final Map<Long, File> catalogFiles = new HashMap<>();
    private final Set<Long> preparedTransactions = new HashSet<>();
    private final Map<IngestFileWriter, FileAggregation> fileAggregations =
            new IdentityHashMap<>();
    private final Set<String> fileContributions = new HashSet<>();
    private long planBytes;
    private final Map<Long, Long> intentBytes = new HashMap<>();

    private static final class FileAggregation {
        private long fileId;
        private long rows;
        private long bytes;
        private long oldestContributionMillis;
        private boolean flushing;
    }

    public PixelsIngestInstaller(
            InstallationStateStore state,
            IngestOptions options,
            String owner,
            RetinaResourceManager resources,
            IndexService indexes,
            MetadataService metadata)
            throws Exception {
        this.state = state;
        this.options = options;
        this.owner = owner;
        this.resources = resources;
        this.indexes = indexes;
        this.metadata = metadata;
        Map<String, BatchInstall> recoveredPlans = state.plans();
        if (!recoveredPlans.isEmpty()) {
            for (BatchInstall plan : recoveredPlans.values()) {
                String key = key(plan);
                if (plans.put(key, plan) != null) {
                    throw new IOException("Duplicate installation plan");
                }
                long rows = 0;
                for (BufferSpan span : plan.getSpansList()) {
                    if (span.getRowCount() <= 0
                            || span.getRowIdStart() != plan.getRowIdStart() + rows) {
                        throw new IOException("Invalid recorded rowId span");
                    }
                    rows += span.getRowCount();
                }
                if (rows > plan.getRowCount()) {
                    throw new IOException("Plan exceeds its batch");
                }
            }
        }
        Map<Long, TransactionCheckpoint> recoveredCheckpoints = state.checkpoints();
        if (!recoveredCheckpoints.isEmpty()) {
            for (TransactionCheckpoint checkpoint : recoveredCheckpoints.values()) {
                if (checkpoint.getTransactionId() <= 0
                        || checkpoint.getCommitTimestamp() <= 0
                        || checkpoints.put(checkpoint.getTransactionId(), checkpoint) != null) {
                    throw new IOException("Invalid or duplicate installation checkpoint");
                }
                validateCheckpoint(checkpoint);
            }
        }
        planBytes = state.size();
    }

    static String key(BatchInstall plan) {
        return IngestWire.batchKey(IngestWire.decode(plan.getStream()), plan.getSequence());
    }

    /** Files whose exact row locations can be reconstructed from retained batch plans. */
    public static Set<Long> recoveryFileIds(InstallationStateStore state) throws IOException {
        Set<Long> fileIds = new HashSet<>();
        for (BatchInstall plan : state.plans().values()) {
            for (BufferSpan span : plan.getSpansList()) {
                fileIds.add(span.getFileId());
            }
        }
        return Collections.unmodifiableSet(fileIds);
    }

    private synchronized void save(BatchInstall value) throws IOException {
        state.put(value);
        plans.put(key(value), value);
        planBytes = state.size();
    }

    private synchronized BatchInstall plan(String batchKey) {
        return plans.get(batchKey);
    }

    private List<byte[][]> rows(Transaction tx, MutationBatch batch) throws IOException {
        TableSpec table = IngestWire.table(tx, batch.getStreamId().getTableId());
        if (batch.getSchemaVersion() != table.getSchemaVersion()
                || batch.getPayloadFormat() != ColumnBatchCodec.FORMAT) {
            throw new IOException("Schema or codec mismatch");
        }
        List<byte[][]> result =
                ColumnBatchCodec.decode(
                        batch.getPayload(),
                        batch.getRowCount(),
                        table.getColumnsCount(),
                        options.maxBatchRows,
                        options.maxBatchBytes);
        IngestRows.Validator validator = new IngestRows.Validator(table);
        for (byte[][] row : result) {
            validator.validate(row);
        }
        return result;
    }

    @Override
    public synchronized void prepare(Transaction tx, Iterable<MutationBatch> batches)
            throws Exception {
        if (preparedTransactions.contains(tx.getTransactionId())) {
            return;
        }
        for (TableSpec table : IngestWire.tables(tx)) {
            IngestTables.validate(table);
        }
        long totalRows = 0;
        long reservation = BASE_PLAN_RESERVATION_BYTES;
        for (MutationBatch batch : batches) {
            totalRows = Math.addExact(totalRows, batch.getRowCount());
            reservation =
                    Math.addExact(
                            reservation,
                            BATCH_PLAN_RESERVATION_BYTES
                                    + VISIBILITY_WORD_RESERVATION_BYTES
                                    * ((batch.getRowCount() + VISIBILITY_WORD_BITS - 1)
                                    / VISIBILITY_WORD_BITS + VISIBILITY_PADDING_WORDS));
        }
        if (totalRows > options.maxPreparedRows) {
            throw new IOException("Prepared row limit exceeded");
        }
        long allReserved = reservedBytes.values().stream().mapToLong(Long::longValue).sum();
        if (planBytes + allReserved + reservation > options.maxStateBytes) {
            throw new IOException("Installation plan capacity exhausted");
        }
        Set<String> keys = new HashSet<>();
        long keyBytes = intentBytes.values().stream().mapToLong(Long::longValue).sum();
        for (MutationBatch batch : batches) {
            TableSpec table = IngestWire.table(tx, batch.getStreamId().getTableId());
            TableIndex primary = IngestRows.primary(table);
            for (byte[][] row : rows(tx, batch)) {
                if (primary == null) {
                    continue;
                }
                ByteString encoded = IngestRows.indexKey(primary, row);
                int bucket = RetinaUtils.getBucketIdFromByteBuffer(encoded);
                if (bucket != batch.getStreamId().getShardId()) {
                    throw new IOException("Primary-key routing mismatch");
                }
                keyBytes = Math.addExact(keyBytes,
                        KEY_INTENT_BASE_BYTES
                                + KEY_INTENT_BYTE_MULTIPLIER * encoded.size());
                if (keyBytes > options.maxStateBytes) {
                    throw new IOException("Prepared key-intent memory limit exceeded");
                }
                String identity =
                        table.getTableId()
                                + ":"
                                + primary.getId()
                                + ":"
                                + Base64.getEncoder().encodeToString(encoded.toByteArray());
                if (!keys.add(identity)) {
                    throw new IOException("Duplicate primary key in INSERT");
                }
                Long holder = keyIntents.get(identity);
                if (holder != null && holder != tx.getTransactionId()) {
                    throw new IOException("Primary-key write conflict");
                }
                IndexProto.IndexKey indexKey =
                        IndexProto.IndexKey.newBuilder()
                                .setTableId(table.getTableId())
                                .setIndexId(primary.getId())
                                .setKey(encoded)
                                .setTimestamp(Long.MAX_VALUE)
                                .build();
                IndexOption indexOption =
                        IndexOption.builder()
                                .vNodeId(IndexUtils.getBucketIdFromByteBuffer(encoded))
                                .build();
                if (indexes.lookupUniqueIndex(indexKey, indexOption) != null) {
                    throw new IOException("Primary key already exists");
                }
            }
        }
        // Mutation of the intent table is all-or-nothing after all validation succeeds.
        for (String identity : keys) {
            keyIntents.put(identity, tx.getTransactionId());
        }
        long existingKeyBytes = intentBytes.values().stream().mapToLong(Long::longValue).sum();
        intentBytes.put(tx.getTransactionId(), keyBytes - existingKeyBytes);
        transactionKeys.put(tx.getTransactionId(), keys);
        reservedBytes.put(tx.getTransactionId(), reservation);
        preparedTransactions.add(tx.getTransactionId());
    }

    @Override
    public synchronized void release(long txId) {
        Set<String> keys = transactionKeys.remove(txId);
        if (keys != null) {
            for (String key : keys) {
                keyIntents.remove(key, txId);
            }
        }
        reservedBytes.remove(txId);
        intentBytes.remove(txId);
        preparedTransactions.remove(txId);
    }

    @Override
    public synchronized void initializeRecovery(List<Transaction> transactions) throws Exception {
        Set<Long> committed = new HashSet<>();
        Map<Long, Long> timestamps = new HashMap<>();
        Map<Long, Transaction> authoritative = new HashMap<>();
        for (Transaction tx : transactions) {
            if (IngestWire.committed(tx)) {
                committed.add(tx.getTransactionId());
                timestamps.put(tx.getTransactionId(), tx.getCommitTimestamp());
                authoritative.put(tx.getTransactionId(), tx);
            }
        }
        Set<Long> retiredCheckpoints = new HashSet<>();
        for (TransactionCheckpoint checkpoint : checkpoints.values()) {
            Transaction tx = authoritative.get(checkpoint.getTransactionId());
            if (tx != null && !matchesCheckpoint(tx, checkpoint)) {
                throw new IOException("Installation checkpoint lacks its authoritative PUBLISHED decision");
            }
            if (tx == null) {
                // The coordinator removes a full PUBLISHED decision only after this participant
                // acknowledged the checkpoint. Its absence is therefore the durable prune ack.
                // Do not re-open checkpointed file identities here: a later storage-GC
                // checkpoint may already have retired them and moved surviving stable rowIds.
                retiredCheckpoints.add(checkpoint.getTransactionId());
            } else {
                verifyCheckpointBaseline(checkpoint);
            }
        }
        if (!retiredCheckpoints.isEmpty()) {
            Map<Long, TransactionCheckpoint> retained = new LinkedHashMap<>(checkpoints);
            retiredCheckpoints.forEach(retained::remove);
            state.storeCheckpoints(retained.values());
            checkpoints.clear();
            checkpoints.putAll(retained);
            planBytes = state.size();
        }
        Set<Long> managedFiles = new HashSet<>();
        for (BatchInstall plan : plans.values()) {
            if (!committed.contains(plan.getStream().getTransactionId())
                    || timestamps.get(plan.getStream().getTransactionId())
                            != plan.getCommitTimestamp()) {
                throw new IOException("Installation plan lacks its authoritative COMMIT decision");
            }
            for (BufferSpan span : plan.getSpansList()) {
                managedFiles.add(span.getFileId());
                File file = metadata.getFileById(span.getFileId());
                if (file == null
                        || file.getPathId() != span.getPathId()
                        || !file.getName().equals(span.getFileName())) {
                    throw new IOException(
                            "Acknowledged installation file identity is missing or changed");
                }
                if (file.getType() != File.Type.REGULAR
                        && file.getType() != File.Type.TEMPORARY_INGEST) {
                    throw new IOException(
                            "Ingestion replay does not support rewritten file identities");
                }
                catalogFiles.put(file.getId(), file);
                resources.addVisibility(file.getId(), 0, span.getFileCapacity(), 0L, null, false);
            }
        }
        resources.cleanupOrphanedIngestFiles(managedFiles);
        resources.initializeIngestBaseline(managedFiles);
    }

    @Override
    public synchronized boolean recoveredByCheckpoint(Transaction tx) throws Exception {
        TransactionCheckpoint checkpoint = checkpoints.get(tx.getTransactionId());
        if (checkpoint == null) {
            return false;
        }
        if (!matchesCheckpoint(tx, checkpoint)) {
            throw new IOException("Recovered transaction differs from installation checkpoint");
        }
        return true;
    }

    @Override
    public boolean checkpoint(
            Transaction tx, Iterable<MutationBatch> batches) throws Exception {
        if (tx.getState() != TransactionState.PUBLISHED) {
            return false;
        }
        List<MutationBatch> transactionBatches = new ArrayList<>();
        batches.forEach(transactionBatches::add);
        Map<String, BatchInstall> planSnapshot = new LinkedHashMap<>();
        synchronized (this) {
            if (checkpoints.containsKey(tx.getTransactionId())) {
                return true;
            }
            for (MutationBatch batch : transactionBatches) {
                String batchKey = IngestWire.batchKey(
                        batch.getStreamId(), batch.getSequence());
                BatchInstall plan = plans.get(batchKey);
                if (plan == null
                        || plan.getStream().getTransactionId()
                                != tx.getTransactionId()) {
                    throw new IOException(
                            "Published transaction is missing its installation plan");
                }
                planSnapshot.put(batchKey, plan);
            }
        }
        Set<Long> coveredFiles = new HashSet<>();
        Map<Long, List<RowIdRange>> rowRanges = new TreeMap<>();
        Map<Long, Set<Long>> tableFiles = new TreeMap<>();
        for (MutationBatch batch : transactionBatches) {
            BatchInstall plan = planSnapshot.get(IngestWire.batchKey(
                    batch.getStreamId(), batch.getSequence()));
            TableSpec table = IngestWire.table(tx, batch.getStreamId().getTableId());
            Set<Long> files = tableFiles.computeIfAbsent(table.getTableId(), ignored -> new TreeSet<>());
            if (!verifyMaterializedBatch(tx, table, batch, plan, files)) {
                return false;
            }
            coveredFiles.addAll(files);
            rowRanges.computeIfAbsent(table.getTableId(), ignored -> new ArrayList<>())
                    .add(RowIdRange.newBuilder()
                            .setRowIdStart(plan.getRowIdStart())
                            .setRowCount(plan.getRowCount())
                            .build());
        }
        if (!resources.isIngestRecoveryCheckpointDurable(
                tx.getCommitTimestamp(), coveredFiles)) {
            return false;
        }
        TransactionCheckpoint.Builder checkpoint = TransactionCheckpoint.newBuilder()
                .setTransactionId(tx.getTransactionId())
                .setCommitTimestamp(tx.getCommitTimestamp());
        for (Map.Entry<Long, List<RowIdRange>> entry : rowRanges.entrySet()) {
            TableSpec table = IngestWire.table(tx, entry.getKey());
            checkpoint.addTables(TableCheckpointCoverage.newBuilder()
                    .setTableId(table.getTableId())
                    .setTableFingerprint(table.getFingerprint())
                    .addAllRowRanges(entry.getValue())
                    .addAllFileIds(tableFiles.get(table.getTableId())));
        }
        synchronized (this) {
            if (checkpoints.containsKey(tx.getTransactionId())) {
                return true;
            }
            for (Map.Entry<String, BatchInstall> entry : planSnapshot.entrySet()) {
                if (!entry.getValue().equals(plans.get(entry.getKey()))) {
                    throw new IOException(
                            "Installation plan changed while checkpointing");
                }
            }
            Map<String, BatchInstall> nextPlans = new LinkedHashMap<>(plans);
            nextPlans.values().removeIf(
                    plan -> plan.getStream().getTransactionId()
                            == tx.getTransactionId());
            Map<Long, TransactionCheckpoint> nextCheckpoints =
                    new LinkedHashMap<>(checkpoints);
            nextCheckpoints.put(tx.getTransactionId(), checkpoint.build());
            state.checkpoint(tx.getTransactionId(), nextCheckpoints.values());
            plans.clear();
            plans.putAll(nextPlans);
            installedBatches.removeAll(planSnapshot.keySet());
            checkpoints.clear();
            checkpoints.putAll(nextCheckpoints);
            synchronized (fileAggregations) {
                fileContributions.removeAll(planSnapshot.keySet());
            }
            planBytes = state.size();
        }
        return true;
    }

    private boolean verifyMaterializedBatch(
            Transaction tx,
            TableSpec table,
            MutationBatch batch,
            BatchInstall plan,
            Set<Long> coveredFiles) throws Exception {
        if (plan.getRowCount() != batch.getRowCount()
                || plan.getCommitTimestamp() != tx.getCommitTimestamp()
                || !plan.getDigest().equals(ByteString.copyFrom(batch.getDigest()))) {
            throw new IOException("Checkpoint batch identity mismatch");
        }
        int rowOffset = 0;
        for (BufferSpan span : plan.getSpansList()) {
            if (span.getRowIdStart() != plan.getRowIdStart() + rowOffset
                    || rowOffset + span.getRowCount() > plan.getRowCount()) {
                throw new IOException("Checkpoint span is incomplete or misaligned");
            }
            File file = metadata.getFileById(span.getFileId());
            if (file == null
                    || file.getPathId() != span.getPathId()
                    || !file.getName().equals(span.getFileName())) {
                throw new IOException("Checkpoint installation file identity is missing or changed");
            }
            if (file.getType() != File.Type.REGULAR) {
                return false;
            }
            if (!coveredFiles.add(span.getFileId())) {
                rowOffset += span.getRowCount();
                continue;
            }
            // MainIndex's per-file flush marker is the durable proof for the exact
            // mappings installed from this immutable plan. SinglePointIndex writes
            // are already durable in the existing index service contract.
            if (!indexes.flushMainIndexOfFile(table.getTableId(), span.getFileId())) {
                return false;
            }
            rowOffset += span.getRowCount();
        }
        if (rowOffset != plan.getRowCount()) {
            throw new IOException("Installation plan is not complete enough to checkpoint");
        }
        return true;
    }

    private void verifyCheckpointBaseline(TransactionCheckpoint checkpoint) throws IOException {
        if (!resources.isIngestRecoveryCheckpointDurable(checkpoint.getCommitTimestamp())) {
            throw new IOException("Published recovery baseline predates installation checkpoint");
        }
    }

    private static void validateCheckpoint(TransactionCheckpoint checkpoint) throws IOException {
        if (checkpoint.getTablesCount() == 0) {
            throw new IOException("Installation checkpoint has no table coverage");
        }
        Set<Long> tables = new HashSet<>();
        for (TableCheckpointCoverage table : checkpoint.getTablesList()) {
            Set<Long> fileIds = new HashSet<>(table.getFileIdsList());
            if (table.getTableId() <= 0
                    || table.getTableFingerprint().isEmpty()
                    || !tables.add(table.getTableId())
                    || table.getRowRangesCount() == 0
                    || fileIds.size() != table.getFileIdsCount()
                    || fileIds.contains(0L)) {
                throw new IOException("Invalid installation checkpoint table coverage");
            }
            for (RowIdRange range : table.getRowRangesList()) {
                if (range.getRowIdStart() < 0 || range.getRowCount() <= 0) {
                    throw new IOException("Invalid installation checkpoint row range");
                }
            }
        }
    }

    private static boolean matchesCheckpoint(Transaction tx, TransactionCheckpoint checkpoint)
            throws IOException {
        if (tx.getState() != TransactionState.PUBLISHED
                || checkpoint.getCommitTimestamp() != tx.getCommitTimestamp()) {
            return false;
        }
        for (TableCheckpointCoverage coverage : checkpoint.getTablesList()) {
            TableSpec table = IngestWire.table(tx, coverage.getTableId());
            if (!table.getFingerprint().equals(coverage.getTableFingerprint())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean install(
            Transaction tx,
            Iterable<MutationBatch> batches,
            boolean recovering,
            boolean forceFileTail)
            throws Exception {
        Map<IngestFileWriter, Set<Long>> requiredFiles = new IdentityHashMap<>();
        for (MutationBatch batch : batches) {
            TableSpec table = IngestWire.table(tx, batch.getStreamId().getTableId());
            Route route = IngestWire.route(table, batch.getStreamId().getShardId());
            if (!IngestWire.owner(route).equals(owner)) {
                throw new IOException("Incorrect participant owner");
            }
            if (tx.getRepresentation() == WriteRepresentation.FILE) {
                IngestFileWriter writer = resources.getIngestFileWriter(
                        table.getSchemaName(), table.getTableName(), route.getVirtualNodeId());
                BatchInstall completed = installFileBatch(
                        tx, table, batch, writer, recovering);
                Set<Long> files = requiredFiles.computeIfAbsent(
                        writer, ignored -> new LinkedHashSet<>());
                Set<Long> batchFiles = new LinkedHashSet<>();
                for (BufferSpan span : completed.getSpansList()) {
                    files.add(span.getFileId());
                    batchFiles.add(span.getFileId());
                }
                if (!allRegular(batchFiles)) {
                    long activeFileId = writer.getActiveFileId();
                    long activeRows = completed.getSpansList().stream()
                            .filter(span -> span.getFileId() == activeFileId)
                            .mapToLong(BufferSpan::getRowCount)
                            .sum();
                    long activeBytes = activeRows == 0
                            ? 0
                            : Math.max(1L,
                                    ((long) batch.getPayloadBytes() * activeRows
                                            + batch.getRowCount() - 1L)
                                            / batch.getRowCount());
                    contributeToFile(writer, key(completed), activeBytes);
                }
            }
            else {
                PixelsWriteBuffer buffer = resources.getIngestBuffer(
                        table.getSchemaName(), table.getTableName(), route.getVirtualNodeId());
                installBufferedBatch(tx, table, batch, buffer, recovering);
            }
        }
        if (tx.getRepresentation() == WriteRepresentation.FILE) {
            for (Map.Entry<IngestFileWriter, Set<Long>> entry : requiredFiles.entrySet()) {
                if (forceFileTail) {
                    flushAggregation(entry.getKey(), true);
                }
                if (!filesReady(entry.getKey(), entry.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    private BatchInstall initialPlan(Transaction tx, MutationBatch batch) throws Exception {
        String batchKey = IngestWire.batchKey(batch.getStreamId(), batch.getSequence());
        BatchInstall plan = plan(batchKey);
        if (plan == null) {
            IndexProto.RowIdBatch allocation =
                    indexes.allocateRowIdBatch(batch.getStreamId().getTableId(), batch.getRowCount());
            if (allocation == null
                    || allocation.getLength() < batch.getRowCount()
                    || allocation.getRowIdStart() < 0) {
                throw new IOException("Existing row allocator returned an insufficient range");
            }
            Math.addExact(allocation.getRowIdStart(), batch.getRowCount() - 1L);
            plan = BatchInstall.newBuilder()
                    .setStream(IngestWire.encode(batch.getStreamId()))
                    .setSequence(batch.getSequence())
                    .setCommitTimestamp(tx.getCommitTimestamp())
                    .setRowIdStart(allocation.getRowIdStart())
                    .setRowCount(batch.getRowCount())
                    .setDigest(ByteString.copyFrom(batch.getDigest()))
                    .build();
            save(plan);
        }
        if (plan.getCommitTimestamp() != tx.getCommitTimestamp()
                || plan.getRowCount() != batch.getRowCount()
                || !plan.getDigest().equals(ByteString.copyFrom(batch.getDigest()))) {
            throw new IOException("Batch installation identity mismatch");
        }
        return plan;
    }

    private BatchInstall installFileBatch(
            Transaction tx,
            TableSpec table,
            MutationBatch batch,
            IngestFileWriter writer,
            boolean recovering) throws Exception {
        synchronized (writer) {
            BatchInstall plan = initialPlan(tx, batch);
            if (isInstalled(plan)) {
                return plan;
            }
            List<byte[][]> rows = rows(tx, batch);
            int offset = 0;
            for (BufferSpan span : plan.getSpansList()) {
                installFileSpan(tx, table, writer, span,
                        rows.subList(offset, offset + span.getRowCount()), recovering, false);
                offset += span.getRowCount();
            }
            while (offset < rows.size()) {
                BufferSpan span = writer.planSpan(
                        rows.size() - offset, Math.addExact(plan.getRowIdStart(), offset));
                plan = plan.toBuilder().addSpans(span).build();
                save(plan);
                installFileSpan(tx, table, writer, span,
                        rows.subList(offset, offset + span.getRowCount()), false, true);
                offset += span.getRowCount();
            }
            if (plan.getSpansCount() == 0) {
                throw new IOException("FILE installation has no durable placement plan");
            }
            markInstalled(plan);
            return plan;
        }
    }

    private void installBufferedBatch(
            Transaction tx,
            TableSpec table,
            MutationBatch batch,
            PixelsWriteBuffer buffer,
            boolean recovering) throws Exception {
        synchronized (buffer) {
            buffer.beginInstallation();
            try {
                BatchInstall plan = initialPlan(tx, batch);
                if (isInstalled(plan)) {
                    return;
                }
                List<byte[][]> rows = rows(tx, batch);
                int offset = 0;
                for (BufferSpan span : plan.getSpansList()) {
                    installBufferedSpan(tx, table, buffer, span,
                            rows.subList(offset, offset + span.getRowCount()), recovering, false);
                    offset += span.getRowCount();
                }
                while (offset < rows.size()) {
                    BufferSpan span = buffer.planSpan(
                            rows.size() - offset, Math.addExact(plan.getRowIdStart(), offset));
                    plan = plan.toBuilder().addSpans(span).build();
                    save(plan);
                    installBufferedSpan(tx, table, buffer, span,
                            rows.subList(offset, offset + span.getRowCount()), false, true);
                    offset += span.getRowCount();
                }
                markInstalled(plan);
            }
            finally {
                buffer.endInstallation();
            }
        }
    }

    @Override
    public long installPollMillis() {
        return options.filePollMillis;
    }

    private void contributeToFile(
            IngestFileWriter writer, String batchKey, long bytes)
            throws Exception {
        if (bytes <= 0) {
            return;
        }
        boolean thresholdReached;
        synchronized (fileAggregations) {
            if (!fileContributions.add(batchKey)) {
                return;
            }
            FileAggregation aggregation = fileAggregations.computeIfAbsent(
                    writer, ignored -> new FileAggregation());
            long activeFileId = writer.getActiveFileId();
            if (activeFileId == 0L) {
                aggregation.fileId = 0L;
                aggregation.rows = 0L;
                aggregation.bytes = 0L;
                aggregation.oldestContributionMillis = 0L;
                return;
            }
            if (aggregation.fileId != activeFileId) {
                aggregation.fileId = activeFileId;
                aggregation.bytes = 0L;
                aggregation.oldestContributionMillis = System.currentTimeMillis();
            }
            aggregation.rows = writer.getActiveRowCount();
            aggregation.bytes = Math.addExact(aggregation.bytes, bytes);
            thresholdReached = aggregation.rows >= options.fileTargetRows
                    || aggregation.bytes >= options.fileMaxBytes;
        }
        if (thresholdReached) {
            flushAggregation(writer, false);
        }
    }

    private boolean filesReady(IngestFileWriter writer, Set<Long> fileIds) throws Exception {
        boolean expired = false;
        synchronized (fileAggregations) {
            FileAggregation aggregation = fileAggregations.get(writer);
            expired = aggregation != null
                    && aggregation.rows > 0
                    && System.currentTimeMillis() - aggregation.oldestContributionMillis
                            >= options.fileMaxDelayMillis;
        }
        if (expired) {
            flushAggregation(writer, true);
        }
        return allRegular(fileIds);
    }

    private boolean allRegular(Set<Long> fileIds) throws Exception {
        for (long fileId : fileIds) {
            File file = metadata.getFileById(fileId);
            if (file == null || file.getType() != File.Type.REGULAR) {
                return false;
            }
        }
        return true;
    }

    private void flushAggregation(IngestFileWriter writer, boolean delayExpired)
            throws RetinaException {
        synchronized (fileAggregations) {
            FileAggregation aggregation = fileAggregations.get(writer);
            if (aggregation == null || aggregation.rows == 0 || aggregation.flushing
                    || (!delayExpired
                            && aggregation.rows < options.fileTargetRows
                            && aggregation.bytes < options.fileMaxBytes)) {
                return;
            }
            aggregation.flushing = true;
        }
        boolean retired = writer.publishTail();
        synchronized (fileAggregations) {
            FileAggregation aggregation = fileAggregations.get(writer);
            if (retired) {
                aggregation.fileId = 0L;
                aggregation.rows = 0L;
                aggregation.bytes = 0L;
                aggregation.oldestContributionMillis = 0L;
            }
            aggregation.flushing = false;
        }
    }

    private void installBufferedSpan(
            Transaction tx,
            TableSpec table,
            PixelsWriteBuffer buffer,
            BufferSpan span,
            List<byte[][]> rows,
            boolean recovering,
            boolean freshSpan)
            throws Exception {
        File file =
                recovering
                        ? catalogFiles.get(span.getFileId())
                        : metadata.getFileById(span.getFileId());
        if (file == null) {
            file = metadata.getFileById(span.getFileId());
        }
        if (file == null) {
            throw new IOException("Installation file was removed");
        }
        boolean published = file.getType() == File.Type.REGULAR;
        if (published) {
            if (recovering) {
                buffer.observePublishedSpan(span);
            }
            // MainIndex explicitly forbids putting a file again after flush.
        } else {
            buffer.installSpan(span, rows, tx.getCommitTimestamp());
            List<IndexProto.PrimaryIndexEntry> locations = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                IndexProto.RowLocation location =
                        IndexProto.RowLocation.newBuilder()
                                .setFileId(span.getFileId())
                                .setRgId(0)
                                .setRgRowOffset(
                                        span.getBlockStartOffset() + span.getOffsetInBlock() + i)
                                .build();
                locations.add(
                        IndexProto.PrimaryIndexEntry.newBuilder()
                                .setRowId(span.getRowIdStart() + i)
                                .setRowLocation(location)
                                .build());
            }
            if (freshSpan) {
                indexes.putMainIndexEntriesOnly(table.getTableId(), locations);
            } else {
                // A replay may follow the per-file MainIndex flush but precede catalog
                // publication; verify recorded positions before rebuilding its cache.
                putMissingMainIndexEntries(table.getTableId(), locations);
            }
        }
        installBusinessIndexes(tx, table, span, rows);
    }

    private void installFileSpan(
            Transaction tx,
            TableSpec table,
            IngestFileWriter writer,
            BufferSpan span,
            List<byte[][]> rows,
            boolean recovering,
            boolean freshSpan) throws Exception {
        File file = recovering ? catalogFiles.get(span.getFileId()) : null;
        if (file == null) {
            file = metadata.getFileById(span.getFileId());
        }
        if (file == null) {
            throw new IOException("Direct installation file was removed");
        }
        boolean published = file.getType() == File.Type.REGULAR;
        if (!published) {
            writer.append(span, rows, tx.getCommitTimestamp());
            List<IndexProto.PrimaryIndexEntry> locations = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                locations.add(IndexProto.PrimaryIndexEntry.newBuilder()
                        .setRowId(span.getRowIdStart() + i)
                        .setRowLocation(IndexProto.RowLocation.newBuilder()
                                .setFileId(span.getFileId())
                                .setRgId(0)
                                .setRgRowOffset(span.getBlockStartOffset() + i)
                                .build())
                        .build());
            }
            if (freshSpan) {
                indexes.putMainIndexEntriesOnly(table.getTableId(), locations);
            } else {
                putMissingMainIndexEntries(table.getTableId(), locations);
            }
        }
        installBusinessIndexes(tx, table, span, rows);
        if (!published) {
            writer.publishIfFull();
        }
    }

    private void putMissingMainIndexEntries(
            long tableId, List<IndexProto.PrimaryIndexEntry> locations) throws Exception {
        List<Long> rowIds = new ArrayList<>(locations.size());
        for (IndexProto.PrimaryIndexEntry entry : locations) {
            rowIds.add(entry.getRowId());
        }
        // Existing spans need a positional replay check; new spans are inserted in bulk.
        List<IndexProto.RowLocation> existing = indexes.lookupRowLocations(tableId, rowIds);
        if (existing.size() != locations.size()) {
            throw new IOException("MainIndex lookup lost positional alignment");
        }
        List<IndexProto.PrimaryIndexEntry> missing = new ArrayList<>();
        for (int i = 0; i < locations.size(); i++) {
            if (existing.get(i) == null) {
                missing.add(locations.get(i));
            }
            else if (!existing.get(i).equals(locations.get(i).getRowLocation())) {
                throw new IOException("Recorded rowId resolves to another storage location");
            }
        }
        if (!missing.isEmpty()) {
            indexes.putMainIndexEntriesOnly(tableId, missing);
        }
    }

    private void installBusinessIndexes(
            Transaction tx, TableSpec table, BufferSpan span, List<byte[][]> rows)
            throws Exception {
        for (TableIndex index : table.getIndexesList()) {
            Map<Integer, List<IndexProto.PrimaryIndexEntry>> primary = new HashMap<>();
            Map<Integer, List<IndexProto.SecondaryIndexEntry>> secondary = new HashMap<>();
            for (int i = 0; i < rows.size(); i++) {
                ByteString key = IngestRows.indexKey(index, rows.get(i));
                int bucket = IndexUtils.getBucketIdFromByteBuffer(key);
                IndexProto.IndexKey version =
                        IndexProto.IndexKey.newBuilder()
                                .setTableId(table.getTableId())
                                .setIndexId(index.getId())
                                .setKey(key)
                                .setTimestamp(tx.getCommitTimestamp())
                                .build();
                if (index.getPrimary()) {
                    primary.computeIfAbsent(bucket, k -> new ArrayList<>())
                            .add(
                                    IndexProto.PrimaryIndexEntry.newBuilder()
                                            .setIndexKey(version)
                                            .setRowId(span.getRowIdStart() + i)
                                            .build());
                } else {
                    secondary
                            .computeIfAbsent(bucket, k -> new ArrayList<>())
                            .add(
                                    IndexProto.SecondaryIndexEntry.newBuilder()
                                            .setIndexKey(version)
                                            .setRowId(span.getRowIdStart() + i)
                                            .build());
                }
            }
            for (Map.Entry<Integer, List<IndexProto.PrimaryIndexEntry>> entry :
                    primary.entrySet()) {
                indexes.putPrimaryIndexEntriesOnly(
                        table.getTableId(),
                        index.getId(),
                        entry.getValue(),
                        IndexOption.builder().vNodeId(entry.getKey()).build());
            }
            for (Map.Entry<Integer, List<IndexProto.SecondaryIndexEntry>> entry :
                    secondary.entrySet()) {
                indexes.putSecondaryIndexEntries(
                        table.getTableId(),
                        index.getId(),
                        entry.getValue(),
                        IndexOption.builder().vNodeId(entry.getKey()).build());
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        installedBatches.clear();
        state.close();
    }

    private synchronized boolean isInstalled(BatchInstall plan) {
        return installedBatches.contains(key(plan));
    }

    private synchronized void markInstalled(BatchInstall plan) {
        // Only a fully installed batch may bypass replay within this process. Recovery
        // starts with an empty set and validates every retained durable plan again.
        installedBatches.add(key(plan));
    }
}
