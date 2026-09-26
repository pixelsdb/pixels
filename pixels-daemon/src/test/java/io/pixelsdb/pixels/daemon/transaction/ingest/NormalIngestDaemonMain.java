/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels and is licensed under the GNU Affero General
 * Public License, version 3 or (at your option) any later version.
 */
package io.pixelsdb.pixels.daemon.transaction.ingest;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.ingest.MutationBatch;
import io.pixelsdb.pixels.common.ingest.MutationStreamId;
import io.pixelsdb.pixels.common.ingest.MutationStreamSeal;
import io.pixelsdb.pixels.common.ingest.durable.AtomicStateFile;
import io.pixelsdb.pixels.common.ingest.rpc.IngestClient;
import io.pixelsdb.pixels.common.ingest.rpc.IngestOptions;
import io.pixelsdb.pixels.common.ingest.wire.ColumnBatchCodec;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.NodeProto;
import io.pixelsdb.pixels.daemon.NodeServiceGrpc;
import io.pixelsdb.pixels.daemon.ServerContainer;
import io.pixelsdb.pixels.daemon.retina.RetinaServer;
import io.pixelsdb.pixels.daemon.transaction.TransServer;
import io.pixelsdb.pixels.ingest.IngestProto.AllocateWriterRequest;
import io.pixelsdb.pixels.ingest.IngestProto.BeginWriteRequest;
import io.pixelsdb.pixels.ingest.IngestProto.CommitAckMode;
import io.pixelsdb.pixels.ingest.IngestProto.CompleteStatementRequest;
import io.pixelsdb.pixels.ingest.IngestProto.InstallationSnapshot;
import io.pixelsdb.pixels.ingest.IngestProto.PrepareWriteRequest;
import io.pixelsdb.pixels.ingest.IngestProto.ReadPin;
import io.pixelsdb.pixels.ingest.IngestProto.Transaction;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionScope;
import io.pixelsdb.pixels.ingest.IngestProto.TransactionState;
import io.pixelsdb.pixels.ingest.IngestProto.VisibleBarrierRequest;
import io.pixelsdb.pixels.ingest.IngestProto.WriteRepresentation;
import io.pixelsdb.pixels.ingest.IngestProto.WriterAssignment;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.retina.RetinaWorkerServiceGrpc;
import io.pixelsdb.pixels.retina.ingest.LocalMutationJournal;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Process-level verification of the production daemon service classes.
 *
 * <p>Only metadata catalog and topology discovery are fixtures. Transaction identity comes from
 * the separately launched real etcd process; TransServer, RetinaServer, ServerContainer, RPC,
 * journal, installer, buffer, SQLite MainIndex, visibility and Pixels file materialization are
 * production implementations.</p>
 */
public final class NormalIngestDaemonMain
{
    private static final long STATEMENT_ID = 1L;
    private static final long FIRST_STATEMENT_ORDINAL = 1L;
    private static final long READ_TIMESTAMP = 0L;
    private static final long FIRST_BATCH_SEQUENCE = 0L;
    private static final long SINGLE_BATCH_COUNT = 1L;
    private static final int MAX_ENCODED_BATCH_BYTES = 1024 * 1024;
    private static final long INGEST_RPC_TIMEOUT_SECONDS = 30L;
    private static final long VISIBILITY_BARRIER_TIMEOUT_MILLIS =
            TimeUnit.SECONDS.toMillis(INGEST_RPC_TIMEOUT_SECONDS);
    private static final int FILE_FIRST_TRANSACTION_ROWS = 3;
    private static final int FILE_SECOND_TRANSACTION_ROWS = 5;
    private static final int FILE_TARGET_ROWS =
            FILE_FIRST_TRANSACTION_ROWS + FILE_SECOND_TRANSACTION_ROWS;
    private static final int SHARED_FILE_TRANSACTION_COUNT = 2;
    private static final int CHECKPOINTED_BUFFERED_ROWS = 64;
    private static final int REPLAY_BUFFERED_ROWS = 1;
    private static final int CUTOVER_ADDED_ROWS = 1;
    private static final int PHASE_ONE_ROWS = FILE_TARGET_ROWS
            + CHECKPOINTED_BUFFERED_ROWS + REPLAY_BUFFERED_ROWS;
    private static final int FILE_MAX_BYTES = 1024 * 1024;
    private static final long FILE_MAX_DELAY_MILLIS = TimeUnit.MINUTES.toMillis(1L);
    private static final Pattern JOURNAL_SEGMENT_PATTERN =
            Pattern.compile("mutations\\.([1-9][0-9]*)\\.wal");

    private static final long CUTOVER_BASELINE = 1_000_000_000L;

    private NormalIngestDaemonMain() {}

    public static void main(String[] args)
    {
        int exit = 0;
        try
        {
            if (args.length != 5)
            {
                throw new IllegalArgumentException(
                        "usage: ROOT ETCD_PORT RETINA_PORT TRANSACTION_PORT "
                                + "write|recover|fail-closed|cutover-reject|cutover");
            }
            run(Paths.get(args[0]), Integer.parseInt(args[1]),
                    Integer.parseInt(args[2]), Integer.parseInt(args[3]),
                    Phase.parse(args[4]));
        }
        catch (Throwable failure)
        {
            failure.printStackTrace(System.err);
            exit = 1;
        }
        System.exit(exit);
    }

    private static void run(Path root, int etcdPort, int retinaPort,
                            int transactionPort, Phase phase) throws Exception
    {
        Files.createDirectories(root);
        Files.createDirectories(root.resolve("ordered"));
        Files.createDirectories(root.resolve("compact"));
        String host = NetUtils.getLocalHostName();
        String owner = host + ":" + retinaPort;
        Path secretFile = root.resolve("credential");
        String secret;
        if (Files.exists(secretFile))
        {
            secret = new String(Files.readAllBytes(secretFile), StandardCharsets.UTF_8).trim();
        }
        else
        {
            secret = "normal-daemon-" + UUID.randomUUID();
            Files.write(secretFile, secret.getBytes(StandardCharsets.UTF_8));
        }

        ConfigFactory config = ConfigFactory.Instance();
        setting(config, "retina.enable", "true");
        setting(config, "retina.ingest.enabled", "true");
        setting(config, "retina.ingest.auth.secret.file", secretFile.toString());
        setting(config, "retina.ingest.coordinator.state.dir", root.resolve("decisions").toString());
        setting(config, "retina.ingest.participant.plan.dir", root.resolve("plans").toString());
        setting(config, "retina.ingest.participant.wal.dir", root.resolve("wal").toString());
        boolean cutover = phase == Phase.CUTOVER || phase == Phase.CUTOVER_REJECT;
        setting(config, "retina.ingest.cutover.baseline.timestamp",
                Long.toString(cutover ? CUTOVER_BASELINE : 0));
        setting(config, "retina.ingest.transaction.lease.ms", "30000");
        setting(config, "retina.ingest.terminal.retention.ms", "30000");
        setting(config, "retina.ingest.file.target.rows", Integer.toString(FILE_TARGET_ROWS));
        setting(config, "retina.ingest.file.max.bytes", Integer.toString(FILE_MAX_BYTES));
        setting(config, "retina.ingest.file.max.delay.ms",
                Long.toString(FILE_MAX_DELAY_MILLIS));
        setting(config, "retina.server.host", host);
        setting(config, "retina.server.port", Integer.toString(retinaPort));
        setting(config, "trans.server.host", "127.0.0.1");
        setting(config, "trans.server.port", Integer.toString(transactionPort));
        setting(config, "retina.ingest.coordinator.host", "127.0.0.1");
        setting(config, "retina.ingest.coordinator.port", Integer.toString(transactionPort));
        setting(config, "etcd.hosts", "127.0.0.1");
        setting(config, "etcd.port", Integer.toString(etcdPort));
        if (phase == Phase.CUTOVER)
        {
            // The supported operator procedure performs a guarded/CAS advance while all
            // writers are drained. This isolated etcd has no concurrent writer, so a direct
            // assignment models the already-completed administrative step.
            EtcdUtil.Instance().putKeyValue(
                    Constants.AI_TRANS_ID_KEY, Long.toString(CUTOVER_BASELINE + 1));
        }
        setting(config, "retina.storage.gc.enabled", "false");
        setting(config, "retina.gc.interval", "1");
        setting(config, "retina.buffer.memTable.size", "64");
        setting(config, "retina.buffer.flush.count", "1");
        // The daemon lifecycle test checks immediate buffer visibility before
        // shutdown and physical row preservation after shutdown. Keep the idle
        // flush scheduler outside that short assertion window; the full SQL test
        // independently exercises the configured buffer-to-file transition.
        setting(config, "retina.buffer.flush.interval", "60");
        setting(config, "retina.buffer.object.storage.folder", root.resolve("objects").toUri().toString());
        setting(config, "retina.storage.gc.journal.dir", root.resolve("gc").toUri().toString());
        setting(config, "retina.offload.checkpoint.dir", root.resolve("offload").toUri().toString());
        setting(config, "retina.recovery.checkpoint.dir", root.resolve("recovery").toUri().toString());
        setting(config, "index.sqlite.path", root.resolve("sqlite").toString());
        setting(config, "enabled.storage.schemes", "file");
        setting(config, "node.bucket.num", "1");
        setting(config, "node.virtual.num", "1");
        setting(config, "index.bucket.num", "1");
        setting(config, "index.cache.enabled", "false");
        setting(config, "cache.enabled", "false");
        setting(config, "projection.read.enabled", "false");
        setting(config, "fixed.split.size", "1");
        setting(config, "scaling.enabled", "false");
        setting(config, "retina.buffer.split.enable", "true");

        PhaseState recoveredState = phase == Phase.WRITE ? null : loadPhaseState(root);
        if (phase == Phase.RECOVER)
        {
            verifyCheckpointReclamation(root, recoveredState);
        }

        SqlIngestFixture.Catalog catalog = new SqlIngestFixture.Catalog(root);
        if (recoveredState != null && countPublishedRows(catalog, root) != PHASE_ONE_ROWS)
        {
            throw new AssertionError("phase-one files do not contain exactly "
                    + PHASE_ONE_ROWS + " rows");
        }
        Server metadata = ServerBuilder.forPort(0).addService(catalog).build().start();
        setting(config, "metadata.server.host", "127.0.0.1");
        setting(config, "metadata.server.port", Integer.toString(metadata.getPort()));

        NodeProto.NodeInfo node = NodeProto.NodeInfo.newBuilder()
                .setAddress(host).setPort(retinaPort).setVirtualNodeId(0).build();
        Server topology = ServerBuilder.forPort(0).addService(new NodeServiceGrpc.NodeServiceImplBase()
        {
            @Override
            public void getRetinaByBucket(NodeProto.GetRetinaByBucketRequest request,
                                          StreamObserver<NodeProto.GetRetinaByBucketResponse> observer)
            {
                reply(observer, NodeProto.GetRetinaByBucketResponse.newBuilder().setNode(node).build());
            }

            @Override
            public void getRetinaList(Empty request, StreamObserver<NodeProto.GetRetinaListResponse> observer)
            {
                reply(observer, NodeProto.GetRetinaListResponse.newBuilder().addNodes(node).build());
            }
        }).build().start();
        setting(config, "node.server.host", "127.0.0.1");
        setting(config, "node.server.port", Integer.toString(topology.getPort()));

        ServerContainer container = new ServerContainer();
        IngestClient client = null;
        long cutoverCommitTimestamp = -1;
        try
        {
            TransServer transaction = new TransServer(transactionPort);
            container.addServer("transaction", transaction);
            awaitRunning(container, "transaction", transaction);
            RetinaServer retina = new RetinaServer(retinaPort);
            container.addServer("retina", retina);
            awaitRunning(container, "retina", retina);

            client = new IngestClient("127.0.0.1", transactionPort, secret,
                    64 * 1024 * 1024, 30000);
            awaitParticipantReady(client, owner);
            if (phase == Phase.WRITE)
            {
                InsertResult firstFile = insertRows(client, FILE_FIRST_TRANSACTION_ROWS,
                        "file-first", 1, WriteRepresentation.FILE, CommitAckMode.DURABLE);
                InsertResult secondFile = insertRows(client, FILE_SECOND_TRANSACTION_ROWS,
                        "file-second", 2, WriteRepresentation.FILE, CommitAckMode.DURABLE);
                client.coordinator().flushVisibleBarrier(VisibleBarrierRequest.newBuilder()
                        .setDeadlineMillis(Math.addExact(
                                System.currentTimeMillis(), VISIBILITY_BARRIER_TIMEOUT_MILLIS))
                        .build());
                assertPublished(client, firstFile.transaction.getTransactionId());
                assertPublished(client, secondFile.transaction.getTransactionId());
                awaitPublishedFiles(catalog, 1);

                InsertResult checkpointed = insertRows(client, CHECKPOINTED_BUFFERED_ROWS,
                        "checkpointed", 3, WriteRepresentation.BUFFERED,
                        CommitAckMode.VISIBLE);
                InsertResult replay = insertRows(client, REPLAY_BUFFERED_ROWS,
                        "replay", 4, WriteRepresentation.BUFFERED,
                        CommitAckMode.VISIBLE);
                savePhaseState(root, firstFile, secondFile, checkpointed, replay);
                verifyLegacyFence(retinaPort);
                verifyBufferedRows(retinaPort, replay.transaction.getCommitTimestamp(),
                        REPLAY_BUFFERED_ROWS);
                awaitPublishedFiles(catalog, 2);
                awaitJournalReclamation(root.resolve("wal"), checkpointed.payloadBytes);
                System.out.println("PIXELS_NORMAL_INGEST_DAEMON_PHASE1_PASS rows="
                        + PHASE_ONE_ROWS + " sharedFileTransactions="
                        + SHARED_FILE_TRANSACTION_COUNT + " checkpointedTransaction="
                        + checkpointed.transaction.getTransactionId());
            }
            else if (phase == Phase.RECOVER)
            {
                assertPublished(client, recoveredState.checkpointTransactionId);
                assertPublished(client, recoveredState.replayTransactionId);
            }
            else if (phase == Phase.CUTOVER)
            {
                InsertResult afterCutover = insertRows(
                        client, CUTOVER_ADDED_ROWS, "after-cutover", 3);
                cutoverCommitTimestamp = afterCutover.transaction.getCommitTimestamp();
                if (cutoverCommitTimestamp <= CUTOVER_BASELINE)
                {
                    throw new AssertionError("cutover commit timestamp did not exceed baseline");
                }
                verifyLegacyFence(retinaPort);
                verifyBufferedRows(retinaPort, cutoverCommitTimestamp, CUTOVER_ADDED_ROWS);
            }
            else if (phase == Phase.CUTOVER_REJECT)
            {
                throw new AssertionError(
                        "daemon accepted an allocator value at/below the cutover baseline");
            }
            else
            {
                throw new AssertionError("fail-closed verification unexpectedly reached READY");
            }
        }
        finally
        {
            if (client != null) client.close();
            container.shutdownAll();
            if (!container.awaitTermination(90, TimeUnit.SECONDS))
            {
                throw new IllegalStateException("normal daemon services did not stop");
            }
            topology.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            metadata.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            catalog.close();
        }
        long rows = countPublishedRows(catalog, root);
        long expectedRows = phase == Phase.CUTOVER
                ? PHASE_ONE_ROWS + CUTOVER_ADDED_ROWS : PHASE_ONE_ROWS;
        if (rows != expectedRows)
        {
            throw new AssertionError("graceful shutdown/restart produced " + rows
                    + " physical rows instead of " + expectedRows);
        }
        if (phase == Phase.RECOVER)
        {
            System.out.println("PIXELS_NORMAL_INGEST_DAEMON_PASS rows=" + PHASE_ONE_ROWS
                    + " pixelsFiles="
                    + catalog.publishedFileCount()
                    + " services=TransServer,RetinaServer checkpointRestart=2"
                    + " sharedFileTransactions=" + SHARED_FILE_TRANSACTION_COUNT);
        }
        else if (phase == Phase.CUTOVER)
        {
            System.out.println("PIXELS_NORMAL_INGEST_CUTOVER_PASS oldRows=" + PHASE_ONE_ROWS
                    + " totalRows=" + (PHASE_ONE_ROWS + CUTOVER_ADDED_ROWS)
                    + " baseline=" + CUTOVER_BASELINE
                    + " commitTimestamp=" + cutoverCommitTimestamp + " legacyFence=1");
        }
    }

    private enum Phase
    {
        WRITE("write"),
        RECOVER("recover"),
        FAIL_CLOSED("fail-closed"),
        CUTOVER_REJECT("cutover-reject"),
        CUTOVER("cutover");

        private final String argument;

        Phase(String argument)
        {
            this.argument = argument;
        }

        private static Phase parse(String argument)
        {
            for (Phase phase : values())
            {
                if (phase.argument.equals(argument))
                {
                    return phase;
                }
            }
            throw new IllegalArgumentException("Unknown daemon verification phase: " + argument);
        }
    }

    private static final class InsertResult
    {
        private final Transaction transaction;
        private final int payloadBytes;

        private InsertResult(Transaction transaction, int payloadBytes)
        {
            this.transaction = transaction;
            this.payloadBytes = payloadBytes;
        }
    }

    private static final class PhaseState
    {
        private final long checkpointTransactionId;
        private final long replayTransactionId;
        private final long firstFileTransactionId;
        private final long secondFileTransactionId;
        private final int checkpointPayloadBytes;

        private PhaseState(long checkpointTransactionId, long replayTransactionId,
                           long firstFileTransactionId, long secondFileTransactionId,
                           int checkpointPayloadBytes)
        {
            this.checkpointTransactionId = checkpointTransactionId;
            this.replayTransactionId = replayTransactionId;
            this.firstFileTransactionId = firstFileTransactionId;
            this.secondFileTransactionId = secondFileTransactionId;
            this.checkpointPayloadBytes = checkpointPayloadBytes;
        }
    }

    private static InsertResult insertRows(
            IngestClient client, int count, String label, int taskId) throws Exception
    {
        return insertRows(client, count, label, taskId,
                WriteRepresentation.BUFFERED, CommitAckMode.VISIBLE);
    }

    private static InsertResult insertRows(
            IngestClient client, int count, String label, int taskId,
            WriteRepresentation representation, CommitAckMode ackMode) throws Exception
    {
        Transaction opened = client.coordinator().beginWrite(BeginWriteRequest.newBuilder()
                .setRequestId("normal-daemon-" + label)
                .setSchemaName("s").setTableName("t").setReadTimestamp(READ_TIMESTAMP)
                .setStatementId(STATEMENT_ID)
                .setQueryId("normal-daemon-" + label + "-query")
                .setStatementOrdinal(FIRST_STATEMENT_ORDINAL)
                .setScope(TransactionScope.AUTOCOMMIT)
                .setRepresentation(representation)
                .setAckMode(ackMode)
                .build());
        WriterAssignment writer = client.coordinator().allocateWriter(
                AllocateWriterRequest.newBuilder()
                        .setTransactionId(opened.getTransactionId())
                        .setRequestId("normal-daemon-" + label + "-writer")
                        .setTaskId(taskId)
                        .setStatementId(STATEMENT_ID)
                        .build());
        MutationStreamId stream = new MutationStreamId(
                opened.getTransactionId(), STATEMENT_ID, writer.getWriterId(),
                opened.getTable().getTableId(), 0, MutationStreamId.Kind.APPEND_ROWS);
        List<byte[][]> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            rows.add(new byte[][] {
                    ByteBuffer.allocate(Long.BYTES).putLong(i).array(),
                    label.getBytes(StandardCharsets.UTF_8)
            });
        }
        byte[] payload = ColumnBatchCodec.encode(
                rows, opened.getTable().getColumnsCount(), MAX_ENCODED_BATCH_BYTES);
        MutationBatch batch = new MutationBatch(
                stream, FIRST_BATCH_SEQUENCE, opened.getTable().getSchemaVersion(),
                ColumnBatchCodec.FORMAT, count, payload);
        client.transport(opened.getTable()).append(batch)
                .get(INGEST_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        MutationStreamSeal expected = new MutationStreamSeal(
                stream, SINGLE_BATCH_COUNT, count, payload.length,
                MutationStreamSeal.extendDigest(
                        MutationStreamSeal.emptyDigest(), batch.getDigest()));
        MutationStreamSeal seal =
                client.transport(opened.getTable()).seal(expected)
                        .get(INGEST_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        client.coordinator().completeStatement(CompleteStatementRequest.newBuilder()
                .setTransactionId(opened.getTransactionId())
                .setStatementId(STATEMENT_ID)
                .addSeals(IngestWire.encode(seal))
                .build());
        Transaction prepared = client.coordinator().prepareWrite(
                PrepareWriteRequest.newBuilder()
                        .setTransactionId(opened.getTransactionId()).build());
        if (prepared.getState() != TransactionState.PREPARED)
        {
            throw new AssertionError("transaction did not prepare: " + prepared.getState());
        }
        Transaction committed =
                client.coordinator().commitWrite(IngestWire.id(opened.getTransactionId()));
        TransactionState expectedState = ackMode == CommitAckMode.VISIBLE
                ? TransactionState.PUBLISHED : TransactionState.COMMIT_DECIDED;
        boolean completedAsRequested = committed.getState() == expectedState
                || (ackMode == CommitAckMode.DURABLE
                && committed.getState() == TransactionState.PUBLISHED);
        if (!completedAsRequested)
        {
            throw new AssertionError("transaction finished in " + committed.getState()
                    + " instead of " + expectedState);
        }
        return new InsertResult(committed, payload.length);
    }

    private static void savePhaseState(
            Path root, InsertResult firstFile, InsertResult secondFile,
            InsertResult checkpointed, InsertResult replay) throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty("checkpoint.transaction",
                Long.toString(checkpointed.transaction.getTransactionId()));
        properties.setProperty("checkpoint.payload.bytes",
                Integer.toString(checkpointed.payloadBytes));
        properties.setProperty("replay.transaction",
                Long.toString(replay.transaction.getTransactionId()));
        properties.setProperty("file.first.transaction",
                Long.toString(firstFile.transaction.getTransactionId()));
        properties.setProperty("file.second.transaction",
                Long.toString(secondFile.transaction.getTransactionId()));
        Path target = root.resolve("phase-state.properties");
        Path temporary = root.resolve("phase-state.properties.new");
        try (OutputStream output = Files.newOutputStream(temporary))
        {
            properties.store(output, "Normal daemon checkpoint/restart state");
        }
        Files.move(temporary, target,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private static PhaseState loadPhaseState(Path root) throws Exception
    {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(root.resolve("phase-state.properties")))
        {
            properties.load(input);
        }
        long checkpoint = Long.parseLong(properties.getProperty("checkpoint.transaction", "-1"));
        long replay = Long.parseLong(properties.getProperty("replay.transaction", "-1"));
        long firstFile = Long.parseLong(
                properties.getProperty("file.first.transaction", "-1"));
        long secondFile = Long.parseLong(
                properties.getProperty("file.second.transaction", "-1"));
        int payload = Integer.parseInt(properties.getProperty("checkpoint.payload.bytes", "-1"));
        if (firstFile <= 0 || secondFile <= firstFile || checkpoint <= secondFile
                || replay <= checkpoint || payload <= 0)
        {
            throw new AssertionError("invalid phase-one verification state");
        }
        return new PhaseState(checkpoint, replay, firstFile, secondFile, payload);
    }

    private static void verifyCheckpointReclamation(Path root, PhaseState phase) throws Exception
    {
        IngestOptions options = new IngestOptions();
        try (AtomicStateFile plans = new AtomicStateFile(
                root.resolve("plans"), options.maxStateBytes))
        {
            InstallationSnapshot snapshot = InstallationSnapshot.parseFrom(plans.read());
            java.util.Set<Long> checkpointed = new java.util.HashSet<>();
            checkpointed.add(phase.firstFileTransactionId);
            checkpointed.add(phase.secondFileTransactionId);
            checkpointed.add(phase.checkpointTransactionId);
            boolean payloadPlanRetained = snapshot.getBatchesList().stream()
                    .anyMatch(batch -> checkpointed.contains(
                            batch.getStream().getTransactionId()));
            java.util.Set<Long> retainedCheckpoints = new java.util.HashSet<>();
            snapshot.getCheckpointsList().forEach(checkpoint ->
                    retainedCheckpoints.add(checkpoint.getTransactionId()));
            if (payloadPlanRetained || !retainedCheckpoints.containsAll(checkpointed))
            {
                throw new AssertionError(
                        "checkpoint did not replace all shared-file transaction plans");
            }
        }
        try (LocalMutationJournal journal = new LocalMutationJournal(
                root.resolve("wal"), options.maxBatchBytes,
                options.walMaxBytes, options.walMaxRecords))
        {
            if (journal.getGeneration() <= 0
                    || !journal.getCheckpointedTransactions()
                            .contains(phase.firstFileTransactionId)
                    || !journal.getCheckpointedTransactions()
                            .contains(phase.secondFileTransactionId)
                    || !journal.getCheckpointedTransactions()
                            .contains(phase.checkpointTransactionId))
            {
                throw new AssertionError("WAL payload was not replaced by a checkpoint fence");
            }
            if (journal.getJournalBytes() >= phase.checkpointPayloadBytes)
            {
                throw new AssertionError("checkpointed WAL payload was not physically reclaimed");
            }
            try (java.util.stream.Stream<Path> paths = Files.list(root.resolve("wal")))
            {
                long generations = paths.filter(path ->
                        path.getFileName().toString().matches("mutations(?:\\.[0-9]+)?\\.wal"))
                        .count();
                if (generations != 1)
                {
                    throw new AssertionError("obsolete WAL generations remain: " + generations);
                }
            }
        }
    }

    private static void assertPublished(IngestClient client, long transactionId)
    {
        Transaction recovered = client.coordinator().getWrite(IngestWire.id(transactionId));
        if (recovered.getState() != TransactionState.PUBLISHED)
        {
            throw new AssertionError("recovered transaction is not PUBLISHED: "
                    + transactionId + " " + recovered.getState());
        }
    }

    private static void verifyLegacyFence(int retinaPort) throws Exception
    {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", retinaPort)
                .usePlaintext().build();
        try
        {
            RetinaWorkerServiceGrpc.RetinaWorkerServiceBlockingStub retina =
                    RetinaWorkerServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(30, TimeUnit.SECONDS);
            RetinaProto.UpdateRecordResponse legacyWrite = retina.updateRecord(
                    RetinaProto.UpdateRecordRequest.newBuilder()
                            .setHeader(RetinaProto.RequestHeader.newBuilder()
                                    .setToken("legacy-write-must-fail"))
                            .setSchemaName("s").setVirtualNodeId(0).build());
            if (legacyWrite.getHeader().getErrorCode() == 0)
            {
                throw new AssertionError("legacy Retina write bypassed transactional cutover");
            }
        }
        finally
        {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void verifyBufferedRows(int retinaPort, long timestamp, int expected)
            throws Exception
    {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", retinaPort)
                .usePlaintext().build();
        try
        {
            RetinaProto.GetWriteBufferResponse response =
                    RetinaWorkerServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(30, TimeUnit.SECONDS)
                            .getWriteBuffer(RetinaProto.GetWriteBufferRequest.newBuilder()
                                    .setHeader(RetinaProto.RequestHeader.newBuilder()
                                            .setToken("normal-daemon-read"))
                                    .setSchemaName("s").setTableName("t")
                                    .setVirtualNodeId(0).setTimestamp(timestamp).build());
            if (response.getData().isEmpty())
            {
                throw new AssertionError("published row was not visible in the shared buffer");
            }
            try (VectorizedRowBatch rows =
                    VectorizedRowBatch.deserialize(response.getData().toByteArray()))
            {
                if (rows.size != expected)
                {
                    throw new AssertionError("expected " + expected
                            + " visible buffer row(s), got " + rows.size);
                }
            }
        }
        finally
        {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void awaitPublishedFiles(SqlIngestFixture.Catalog catalog, long expected)
            throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < deadline)
        {
            if (catalog.publishedFileCount() >= expected)
            {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Retina did not publish the expected Pixels file");
    }

    private static void awaitJournalReclamation(Path wal, long checkpointPayloadBytes) throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < deadline)
        {
            try (java.util.stream.Stream<Path> paths = Files.list(wal))
            {
                List<Path> generations = paths.filter(path -> JOURNAL_SEGMENT_PATTERN
                                .matcher(path.getFileName().toString()).matches())
                        .collect(java.util.stream.Collectors.toList());
                if (generations.size() == 1
                        && !Files.exists(wal.resolve("mutations.wal"))
                        && Files.size(generations.get(0)) < checkpointPayloadBytes)
                {
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Checkpointed WAL payload was not physically reclaimed");
    }

    private static long countPublishedRows(SqlIngestFixture.Catalog catalog, Path root)
            throws Exception
    {
        long rows = 0;
        for (MetadataProto.File file : catalog.files.values())
        {
            if (file.getType() != MetadataProto.File.Type.REGULAR)
            {
                continue;
            }
            String path = root.resolve("ordered").resolve(file.getName()).toUri().toString();
            try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(StorageFactory.Instance().getStorage(path))
                    .setPath(path).setPixelsFooterCache(new PixelsFooterCache()).build())
            {
                PixelsReaderOption option = new PixelsReaderOption();
                option.includeCols(new String[] {"id", "label"});
                try (PixelsRecordReader records = reader.read(option))
                {
                    VectorizedRowBatch batch;
                    while ((batch = records.readBatch()) != null && batch.size > 0)
                    {
                        rows += batch.size;
                    }
                }
            }
        }
        return rows;
    }

    private static void awaitParticipantReady(IngestClient client, String owner) throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Throwable last = null;
        while (System.nanoTime() < deadline)
        {
            try
            {
                ReadPin pin = client.participant(owner).pinRead(ReadPin.newBuilder()
                        .setTransactionId(1).setReadTimestamp(0).build());
                client.participant(owner).releaseRead(pin);
                return;
            }
            catch (Throwable failure)
            {
                last = failure;
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("Retina participant never became READY", last);
    }

    private static void awaitRunning(
            ServerContainer container, String name,
            io.pixelsdb.pixels.common.server.Server server) throws Exception
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline)
        {
            if (container.checkServer(name) && server.isRunning()) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException(name + " server thread did not start");
    }

    private static void setting(ConfigFactory config, String key, String value)
    {
        config.addProperty(key, value);
    }

    private static <T> void reply(StreamObserver<T> observer, T value)
    {
        observer.onNext(value);
        observer.onCompleted();
    }
}
