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

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.index.MainIndexFactory;
import io.pixelsdb.pixels.common.index.service.*;
import io.pixelsdb.pixels.common.ingest.*;
import io.pixelsdb.pixels.common.ingest.rpc.*;
import io.pixelsdb.pixels.common.ingest.wire.*;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.ingest.IngestTables;
import io.pixelsdb.pixels.core.reader.*;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.daemon.*;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.pixelsdb.pixels.retina.*;
import io.pixelsdb.pixels.retina.ingest.*;

import org.junit.jupiter.api.Test;

import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Real buffers, native visibility, SQLite MainIndex, local objects and Pixels files.
 * Catalog/node RPCs and the etcd-backed allocation source are isolated test doubles.
 */
public class TestPixelsIngestStorage {
    private static final long STATEMENT_ID = 1L;
    private static final int INSTALLATION_STATE_BYTES = 16 * 1024 * 1024;
    private static final int INSTALLATION_COMPACTION_BYTES = 8 * 1024 * 1024;
    private static final int PLAN_JOURNAL_HEADER_BYTES = 2 * Integer.BYTES;
    private static final int PLAN_FRAME_HEADER_BYTES = 2 * Integer.BYTES;

    private static <T> void reply(StreamObserver<T> out, T value) {
        out.onNext(value);
        out.onCompleted();
    }

    private static MetadataProto.ResponseHeader ok(MetadataProto.RequestHeader request) {
        return MetadataProto.ResponseHeader.newBuilder().setToken(request.getToken()).build();
    }

    static class Catalog extends MetadataServiceGrpc.MetadataServiceImplBase {
        final Map<Long, MetadataProto.File> files = new ConcurrentHashMap<>();
        final AtomicLong ids = new AtomicLong(100);
        final AtomicBoolean rejectPublication = new AtomicBoolean(false);
        final MetadataProto.Layout layout;

        Catalog(Path root) {
            layout =
                    MetadataProto.Layout.newBuilder()
                            .setId(1)
                            .setTableId(73)
                            .setSchemaVersionId(1)
                            .setVersion(1)
                            .setPermission(MetadataProto.Permission.READ_WRITE)
                            .setOrdered("{\"columnOrder\":[\"v\"]}")
                            .setCompact("{}")
                            .setSplits("{}")
                            .setProjections("{}")
                            .addOrderedPaths(
                                    MetadataProto.Path.newBuilder()
                                            .setId(1)
                                            .setLayoutId(1)
                                            .setUri(root.resolve("ordered").toUri().toString())
                                            .setType(MetadataProto.Path.Type.ORDERED))
                            .addCompactPaths(
                                    MetadataProto.Path.newBuilder()
                                            .setId(2)
                                            .setLayoutId(1)
                                            .setUri(root.resolve("compact").toUri().toString())
                                            .setType(MetadataProto.Path.Type.COMPACT))
                            .build();
        }

        /** Called after a catalog mutation and before its successful RPC reply. */
        protected void catalogMutated() {
        }

        private MetadataProto.Table table() {
            return MetadataProto.Table.newBuilder()
                    .setId(73).setName("t").setType("user").setSchemaId(1)
                    .setStorageScheme("file").build();
        }

        public void getSchemas(
                MetadataProto.GetSchemasRequest r,
                StreamObserver<MetadataProto.GetSchemasResponse> o) {
            reply(o, MetadataProto.GetSchemasResponse.newBuilder()
                    .setHeader(ok(r.getHeader()))
                    .addSchemas(MetadataProto.Schema.newBuilder().setId(1).setName("s"))
                    .build());
        }

        public void getTables(
                MetadataProto.GetTablesRequest r,
                StreamObserver<MetadataProto.GetTablesResponse> o) {
            reply(o, MetadataProto.GetTablesResponse.newBuilder()
                    .setHeader(ok(r.getHeader())).addTables(table()).build());
        }

        public void getLayouts(
                MetadataProto.GetLayoutsRequest r,
                StreamObserver<MetadataProto.GetLayoutsResponse> o) {
            reply(o, MetadataProto.GetLayoutsResponse.newBuilder()
                    .setHeader(ok(r.getHeader())).addLayouts(layout).build());
        }

        public void getTable(
                MetadataProto.GetTableRequest r, StreamObserver<MetadataProto.GetTableResponse> o) {
            reply(
                    o,
                    MetadataProto.GetTableResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setTable(table())
                            .addLayouts(layout)
                            .build());
        }

        public void getColumns(
                MetadataProto.GetColumnsRequest r,
                StreamObserver<MetadataProto.GetColumnsResponse> o) {
            reply(
                    o,
                    MetadataProto.GetColumnsResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .addColumns(
                                    MetadataProto.Column.newBuilder()
                                            .setId(1)
                                            .setTableId(73)
                                            .setName("v")
                                            .setType("varbinary"))
                            .build());
        }

        public void getLayout(
                MetadataProto.GetLayoutRequest r,
                StreamObserver<MetadataProto.GetLayoutResponse> o) {
            reply(
                    o,
                    MetadataProto.GetLayoutResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setLayout(layout)
                            .build());
        }

        public void getSinglePointIndices(
                MetadataProto.GetSinglePointIndicesRequest r,
                StreamObserver<MetadataProto.GetSinglePointIndicesResponse> o) {
            reply(
                    o,
                    MetadataProto.GetSinglePointIndicesResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .build());
        }

        public void getPrimaryIndex(
                MetadataProto.GetPrimaryIndexRequest r,
                StreamObserver<MetadataProto.GetPrimaryIndexResponse> o) {
            reply(o, MetadataProto.GetPrimaryIndexResponse.newBuilder()
                    .setHeader(MetadataProto.ResponseHeader.newBuilder()
                            .setToken(r.getHeader().getToken())
                            .setErrorCode(io.pixelsdb.pixels.common.error.ErrorCode
                                    .METADATA_SINGLE_POINT_INDEX_NOT_FOUND)
                            .setErrorMsg("keyless test table").build())
                    .build());
        }

        public void addFiles(
                MetadataProto.AddFilesRequest r, StreamObserver<MetadataProto.AddFilesResponse> o) {
            for (MetadataProto.File f : r.getFilesList()) {
                long id = ids.incrementAndGet();
                files.put(id, f.toBuilder().setId(id).build());
            }
            catalogMutated();
            reply(
                    o,
                    MetadataProto.AddFilesResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .build());
        }

        public void getFileId(
                MetadataProto.GetFileIdRequest r,
                StreamObserver<MetadataProto.GetFileIdResponse> o) {
            long id =
                    files.values().stream()
                            .filter(f -> r.getFilePathUri().endsWith("/" + f.getName()))
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException("missing file"))
                            .getId();
            reply(
                    o,
                    MetadataProto.GetFileIdResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setFileId(id)
                            .build());
        }

        public void getFileById(
                MetadataProto.GetFileByIdRequest r,
                StreamObserver<MetadataProto.GetFileByIdResponse> o) {
            MetadataProto.GetFileByIdResponse.Builder b =
                    MetadataProto.GetFileByIdResponse.newBuilder().setHeader(ok(r.getHeader()));
            if (files.containsKey(r.getFileId())) {
                b.setFile(files.get(r.getFileId()));
            }
            reply(o, b.build());
        }

        public void getFilesByType(
                MetadataProto.GetFilesByTypeRequest r,
                StreamObserver<MetadataProto.GetFilesByTypeResponse> o) {
            MetadataProto.GetFilesByTypeResponse.Builder b =
                    MetadataProto.GetFilesByTypeResponse.newBuilder().setHeader(ok(r.getHeader()));
            files.values().stream()
                    .filter(
                            f ->
                                    r.getFileTypesList().contains(f.getType())
                                            && (!r.hasPathId() || r.getPathId() == f.getPathId()))
                    .forEach(b::addFiles);
            reply(o, b.build());
        }

        public void updateFile(
                MetadataProto.UpdateFileRequest r,
                StreamObserver<MetadataProto.UpdateFileResponse> o) {
            if (rejectPublication.get()
                    && r.getFile().getType() == MetadataProto.File.Type.REGULAR) {
                o.onError(
                        Status.UNAVAILABLE
                                .withDescription("injected catalog publication failure")
                                .asRuntimeException());
                return;
            }
            files.put(r.getFile().getId(), r.getFile());
            catalogMutated();
            reply(
                    o,
                    MetadataProto.UpdateFileResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .build());
        }

        public void deleteFiles(
                MetadataProto.DeleteFilesRequest r,
                StreamObserver<MetadataProto.DeleteFilesResponse> o) {
            r.getFileIdsList().forEach(files::remove);
            catalogMutated();
            reply(
                    o,
                    MetadataProto.DeleteFilesResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .build());
        }

        public void atomicSwapFiles(
                MetadataProto.AtomicSwapFilesRequest r,
                StreamObserver<MetadataProto.AtomicSwapFilesResponse> o) {
            MetadataProto.File replacement = files.get(r.getNewFileId());
            if (replacement == null || !r.hasCleanupAt()) {
                o.onError(Status.FAILED_PRECONDITION.asRuntimeException());
                return;
            }
            files.put(r.getNewFileId(), replacement.toBuilder()
                    .setType(MetadataProto.File.Type.REGULAR).clearCleanupAt().build());
            for (long oldFileId : r.getOldFileIdsList()) {
                MetadataProto.File old = files.get(oldFileId);
                if (old != null) {
                    files.put(oldFileId, old.toBuilder()
                            .setType(MetadataProto.File.Type.RETIRED)
                            .setCleanupAt(r.getCleanupAt()).build());
                }
            }
            catalogMutated();
            reply(o, MetadataProto.AtomicSwapFilesResponse.newBuilder()
                    .setHeader(ok(r.getHeader())).build());
        }
    }

    @Test
    public void realBufferInstallationIsIdempotentAndCoalescesKeylessRows() throws Exception {
        Path root = Files.createTempDirectory("pixels-ingest-storage-");
        Files.createDirectories(root.resolve("ordered"));
        Files.createDirectories(root.resolve("compact"));
        Path secret = root.resolve("credential");
        Files.write(
                secret,
                "test-ingest-storage-secret-01234567890"
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        ConfigFactory config = ConfigFactory.Instance();
        Map<String, String> changes = new LinkedHashMap<>();
        changes.put("retina.enable", "true");
        changes.put("retina.ingest.enabled", "true");
        changes.put("retina.ingest.auth.secret.file", secret.toString());
        changes.put("retina.ingest.coordinator.state.dir", root.resolve("config-decisions").toString());
        changes.put("retina.ingest.participant.plan.dir", root.resolve("config-plans").toString());
        changes.put("retina.ingest.participant.wal.dir", root.resolve("config-wal").toString());
        changes.put("retina.storage.gc.enabled", "false");
        changes.put("retina.buffer.memTable.size", "64");
        changes.put("retina.buffer.flush.count", "2");
        // This test controls file rollover explicitly and asserts the intermediate
        // two-file/one-active-MemTable layout before exercising rewrite GC. Keep
        // the independent idle-flush scheduler outside that assertion window;
        // slow CI hosts can otherwise flush the final four rows into a third file.
        changes.put("retina.buffer.flush.interval", "60");
        changes.put("retina.ingest.file.target.rows", "128");
        changes.put("retina.ingest.file.pixel.stride", "64");
        changes.put("retina.ingest.file.max.delay.ms", "60000");
        changes.put(
                "retina.buffer.object.storage.folder", root.resolve("objects").toUri().toString());
        changes.put("retina.storage.gc.journal.dir", root.resolve("gc").toUri().toString());
        changes.put("retina.offload.checkpoint.dir", root.resolve("offload").toUri().toString());
        changes.put("index.sqlite.path", root.resolve("sqlite").toString());
        changes.put("enabled.storage.schemes", "file");
        changes.put("node.bucket.num", "1");
        changes.put("node.virtual.num", "1");
        changes.put("index.bucket.num", "1");
        changes.put("index.cache.enabled", "false");
        changes.put("cache.enabled", "false");
        Map<String, String> previous = new HashMap<>();
        changes.forEach(
                (k, v) -> {
                    previous.put(k, config.getProperty(k));
                    config.addProperty(k, v);
                });
        Catalog catalog = new Catalog(root);
        Server meta = ServerBuilder.forPort(0).addService(catalog).build().start();
        NodeServiceGrpc.NodeServiceImplBase node =
                new NodeServiceGrpc.NodeServiceImplBase() {
                    public void getRetinaByBucket(
                            NodeProto.GetRetinaByBucketRequest r,
                            StreamObserver<NodeProto.GetRetinaByBucketResponse> o) {
                        reply(
                                o,
                                NodeProto.GetRetinaByBucketResponse.newBuilder()
                                        .setNode(
                                                NodeProto.NodeInfo.newBuilder()
                                                        .setAddress("127.0.0.1")
                                                        .setPort(18890)
                                                        .setVirtualNodeId(0))
                                        .build());
                    }
                };
        Server nodes = ServerBuilder.forPort(0).addService(node).build().start();
        config.addProperty("metadata.server.host", "127.0.0.1");
        config.addProperty("metadata.server.port", Integer.toString(meta.getPort()));
        config.addProperty("node.server.host", "127.0.0.1");
        config.addProperty("node.server.port", Integer.toString(nodes.getPort()));
        PixelsWriteBuffer buffer = null;
        PixelsIngestInstaller installer = null;
        Path planDirectory = Files.createDirectory(root.resolve("plans"));
        try {
            RetinaResourceManager resources = RetinaResourceManager.Instance();
            resources.getIngestReadPins().ready();
            ReadPin pin =
                    resources
                            .getIngestReadPins()
                            .pin(
                                    ReadPin.newBuilder()
                                            .setTransactionId(99)
                                            .setReadTimestamp(0)
                                            .build());
            TableSpec table = IngestTables.load("s", "t");
            assertEquals(0, table.getIndexesCount());
            resources.addWriteBuffer("s", "t");
            buffer = resources.getIngestBuffer("s", "t", 0);
            assertNotSame(
                    buffer,
                    resources.getIngestFileWriter("s", "t", 0),
                    "FILE installation must bypass the query-visible MemTable path");
            AtomicLong allocation = new AtomicLong(1000),
                    allocationCalls = new AtomicLong(),
                    putCalls = new AtomicLong();
            AtomicBoolean failOnce = new AtomicBoolean(true);
            AtomicBoolean rejectCheckpointFlush = new AtomicBoolean();
            AtomicInteger locationLookupCalls = new AtomicInteger();
            AtomicInteger rangePutCalls = new AtomicInteger();
            IndexService delegate = LocalIndexService.Instance();
            InstallationStateStore installationState = new InstallationStateStore(
                    planDirectory, INSTALLATION_STATE_BYTES, INSTALLATION_COMPACTION_BYTES);
            IndexService index =
                    (IndexService)
                            Proxy.newProxyInstance(
                                    getClass().getClassLoader(),
                                    new Class<?>[] {IndexService.class},
                                    (proxy, method, args) -> {
                                        if (method.getName().equals("lookupRowLocations")) {
                                            locationLookupCalls.incrementAndGet();
                                        }
                                        if (method.getName().equals("putMainIndexRangeOnly")) {
                                            rangePutCalls.incrementAndGet();
                                        }
                                        if (method.getName().equals("allocateRowIdBatch")) {
                                            int count = (Integer) args[1];
                                            allocationCalls.incrementAndGet();
                                            return IndexProto.RowIdBatch.newBuilder()
                                                    .setRowIdStart(allocation.getAndAdd(count))
                                                    .setLength(count)
                                                    .build();
                                        }
                                        if (method.getName().equals("flushMainIndexOfFile")
                                                && rejectCheckpointFlush.get()) {
                                            return false;
                                        }
                                        if (method.getName().equals("putMainIndexEntriesOnly")
                                                && putCalls.get() == 0) {
                                            Collection<BatchInstall> durablePlans = installationState.plans().values();
                                            assertEquals(1, durablePlans.size());
                                            BatchInstall first = durablePlans.iterator().next();
                                            assertEquals(1, first.getSpansCount());
                                            assertEquals(first.getRowIdStart(), first.getSpans(0).getRowIdStart());
                                            assertEquals(PLAN_JOURNAL_HEADER_BYTES + PLAN_FRAME_HEADER_BYTES
                                                            + Byte.BYTES + first.getSerializedSize(),
                                                    Files.size(planDirectory.resolve("plans.log")),
                                                    "Allocation and first placement must share one durable record");
                                        }
                                        try {
                                            Object result = method.invoke(delegate, args);
                                            if (method.getName()
                                                    .equals("putMainIndexEntriesOnly")) {
                                                putCalls.incrementAndGet();
                                                if (failOnce.compareAndSet(true, false)) {
                                                    throw new io.pixelsdb.pixels.common.exception
                                                            .IndexException(
                                                            "after real MainIndex write");
                                                }
                                            }
                                            return result;
                                        } catch (InvocationTargetException e) {
                                            throw e.getCause();
                                        }
                                    });
            installer =
                    new PixelsIngestInstaller(
                            installationState,
                            new IngestOptions(),
                            "127.0.0.1:18890",
                            resources,
                            index,
                            MetadataService.Instance());
            List<byte[][]> rows = Collections.nCopies(250, new byte[][] {new byte[] {7}});
            byte[] payload = ColumnBatchCodec.encode(rows, 1, 1024 * 1024);
            MutationBatch batch =
                    new MutationBatch(
                            new MutationStreamId(
                                    100, STATEMENT_ID, 1, 73, 0,
                                    MutationStreamId.Kind.APPEND_ROWS),
                            0,
                            table.getSchemaVersion(),
                            ColumnBatchCodec.FORMAT,
                            rows.size(),
                            payload);
            Transaction tx =
                    Transaction.newBuilder()
                            .setTransactionId(100)
                            .setTable(table)
                            .addEnlistedTables(table)
                            .setCommitTimestamp(200)
                            .setState(TransactionState.COMMIT_DECIDED)
                            .setOutcome(DecisionOutcome.COMMIT)
                            .setProgress(PublicationProgress.INSTALLING)
                            .setCommitToken("storage-test-100")
                            .build();
            // Hold catalog publication at the intended fault boundary. The object and
            // MainIndex flushes remain real, while buffer-read assertions no longer race the
            // asynchronous publisher that this test later resumes explicitly.
            catalog.rejectPublication.set(true);
            AtomicInteger preparePasses = new AtomicInteger();
            installer.prepare(tx, () -> {
                assertEquals(1, preparePasses.incrementAndGet(),
                        "Prepare must not replay WAL payloads just to count rows");
                return Collections.singletonList(batch).iterator();
            });
            assertEquals(1, preparePasses.get());
            assertEquals(0, catalog.files.size());
            PixelsIngestInstaller target = installer;
            assertThrows(
                    io.pixelsdb.pixels.common.exception.IndexException.class,
                    () -> target.install(tx, Collections.singletonList(batch), false, false));
            installer.install(tx, Collections.singletonList(batch), false, false);
            installer.install(tx, Collections.singletonList(batch), false, false);
            assertEquals(
                    1, allocationCalls.get(), "Replay must reuse the recorded allocator result");
            for (long id = 1000; id < 1250; id++) {
                assertNotNull(MainIndexFactory.Instance().getMainIndex(73).getLocation(id));
            }
            assertEquals(
                    2, catalog.files.size(), "Rows, not statement boundaries, roll shared files");
            assertTrue(
                    catalog.files.values().stream()
                            .allMatch(
                                    f -> f.getType() == MetadataProto.File.Type.TEMPORARY_INGEST));
            long visible = bufferedRows(buffer);
            assertEquals(250, visible);
            // A second transaction fills the last block and starts the next generation.
            MutationBatch second =
                    new MutationBatch(
                            new MutationStreamId(
                                    101, STATEMENT_ID, 1, 73, 0,
                                    MutationStreamId.Kind.APPEND_ROWS),
                            0,
                            table.getSchemaVersion(),
                            ColumnBatchCodec.FORMAT,
                            10,
                            ColumnBatchCodec.encode(
                                    Collections.nCopies(10, new byte[][] {new byte[] {7}}),
                                    1,
                                    1024 * 1024));
            Transaction next = tx.toBuilder().setTransactionId(101).setCommitTimestamp(201).build();
            installer.prepare(next, Collections.singletonList(second));
            installer.install(next, Collections.singletonList(second), false, false);
            assertEquals(260, bufferedRows(buffer));
            assertEquals(2, allocationCalls.get());
            // Query the real read overlay before files exist. Use bitmap identities
            // from the same captured version, including objects still spilling.
            assertEquals(250, readBuffered(resources, root, 200));
            IndexProto.RowLocation deleted =
                    MainIndexFactory.Instance().getMainIndex(73).getLocation(1000);
            resources.deleteRecord(deleted, 200);
            assertEquals(249, readBuffered(resources, root, 200));
            assertEquals(259, readBuffered(resources, root, 201));
            assertEquals(0, readBuffered(resources, root, 199));
            long priorPuts = putCalls.get();
            resources.getIngestReadPins().release(pin);
            // Wait until the actual SQLite flush is durable, while metadata publication fails.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline && !hasFlushedFile(root.resolve("sqlite"))) {
                Thread.sleep(25);
            }
            assertTrue(
                    hasFlushedFile(root.resolve("sqlite")),
                    "MainIndex per-file marker must be persisted");
            installer.install(tx, Collections.singletonList(batch), false, false);
            assertEquals(
                    priorPuts,
                    putCalls.get(),
                    "A catalog retry must not re-put already flushed MainIndex entries");
            catalog.rejectPublication.set(false);
            Method flushReadyFiles = PixelsWriteBuffer.class
                    .getDeclaredMethod("flushReadyFilesSafely");
            flushReadyFiles.setAccessible(true);
            flushReadyFiles.invoke(buffer);
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline
                    && catalog.files.values().stream()
                                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR)
                                    .count()
                            < 2) {
                Thread.sleep(25);
            }
            List<MetadataProto.File> regular = new ArrayList<>();
            catalog.files.values().stream()
                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR)
                    .forEach(regular::add);
            assertEquals(2, regular.size());
            int materialized = 0;
            for (MetadataProto.File file : regular) {
                String path = root.resolve("ordered").resolve(file.getName()).toUri().toString();
                try (PixelsReader reader =
                        PixelsReaderImpl.newBuilder()
                                .setStorage(StorageFactory.Instance().getStorage(path))
                                .setPath(path)
                                .setPixelsFooterCache(new PixelsFooterCache())
                                .build()) {
                    PixelsReaderOption option = new PixelsReaderOption();
                    option.includeCols(new String[] {"v"});
                    try (PixelsRecordReader records = reader.read(option)) {
                        VectorizedRowBatch data;
                        while ((data = records.readBatch()) != null && data.size > 0) {
                            materialized += data.size;
                        }
                    }
                }
            }
            assertEquals(256, materialized);
            assertEquals(4, bufferedRows(buffer));
            assertEquals(260, materialized + bufferedRows(buffer));

            List<RecoveryCheckpoint.VisibilityEntry> checkpointVisibility = new ArrayList<>();
            for (MetadataProto.File file : regular) {
                checkpointVisibility.add(new RecoveryCheckpoint.VisibilityEntry(
                        file.getId(), 0, 1, 201, new long[0]));
            }
            resources.adoptRecoveryCheckpoint(
                    RecoveryCheckpoint.Body.builder()
                            .retinaNodeId("test")
                            .writeTimeMs(System.currentTimeMillis())
                            .checkpointAppliedTs(201)
                            .virtualNodesPerNode(1)
                            .rgEntries(checkpointVisibility)
                            .build());
            Transaction published = tx.toBuilder().setState(TransactionState.PUBLISHED)
                    .setProgress(PublicationProgress.VISIBLE_NOW).build();
            rejectCheckpointFlush.set(true);
            Iterable<MutationBatch> blockedCoverage = () -> new java.util.Iterator<MutationBatch>() {
                private boolean delivered;

                public boolean hasNext() { return true; }

                public MutationBatch next() {
                    if (delivered) {
                        throw new AssertionError("Checkpoint must verify coverage before reading more WAL");
                    }
                    delivered = true;
                    return batch;
                }
            };
            assertFalse(installer.checkpoint(published, blockedCoverage),
                    "Failed MainIndex durability proof must retain the installation plan");
            rejectCheckpointFlush.set(false);
            assertTrue(installer.checkpoint(published, Collections.singletonList(batch)));
            assertTrue(installer.checkpoint(published, () -> {
                throw new AssertionError("Completed checkpoint must not require reclaimed WAL");
            }));
            assertTrue(installer.recoveredByCheckpoint(published));

            // Rewrite one real Pixels file from this INSERT. Stable rowIds move only in
            // MainIndex; this keyless table has no business index to synthesize.
            MetadataProto.File rewriteSource = regular.stream()
                    .min(Comparator.comparingLong(MetadataProto.File::getId)).orElseThrow(AssertionError::new);
            long sourceFileId = rewriteSource.getId();
            String sourcePath = root.resolve("ordered").resolve(rewriteSource.getName()).toUri().toString();
            Set<Long> originalRegularIds = new HashSet<>();
            regular.forEach(f -> originalRegularIds.add(f.getId()));
            List<IndexProto.PrimaryIndexEntry> sourceEntries =
                    delegate.getMainIndexEntriesForFiles(73, Collections.singleton(sourceFileId));
            assertFalse(sourceEntries.isEmpty());
            int deleteCount = sourceEntries.size() / 2 + 1;
            Map<String, long[]> gcBitmaps = new HashMap<>();
            Set<Long> deletedRowIds = new HashSet<>();
            for (int i = 0; i < deleteCount; i++) {
                IndexProto.PrimaryIndexEntry entry = sourceEntries.get(i);
                IndexProto.RowLocation location = entry.getRowLocation();
                resources.deleteRecord(location, 202);
                deletedRowIds.add(entry.getRowId());
                String rgKey = io.pixelsdb.pixels.common.utils.RetinaUtils.buildRgKey(
                        sourceFileId, location.getRgId());
                long[] words = gcBitmaps.computeIfAbsent(rgKey,
                        ignored -> new long[(sourceEntries.size() + 63) / 64]);
                words[location.getRgRowOffset() >>> 6] |=
                        1L << (location.getRgRowOffset() & 63);
            }
            Map<Long, long[]> fileStats = Collections.singletonMap(
                    sourceFileId, new long[] {sourceEntries.size(), deleteCount});
            StorageGcWal gcWal = new StorageGcWal();
            Constructor<StorageGarbageCollector> gcConstructor =
                    StorageGarbageCollector.class.getDeclaredConstructor(
                            RetinaResourceManager.class, MetadataService.class, IndexService.class,
                            double.class, long.class, int.class, int.class, int.class,
                            EncodingLevel.class, long.class, StorageGcWal.class);
            gcConstructor.setAccessible(true);
            StorageGarbageCollector storageGc = gcConstructor.newInstance(
                    resources, MetadataService.Instance(), delegate, 0.5, 134_217_728L,
                    16, 10, 1_048_576, EncodingLevel.EL2, 0L, gcWal);
            Method runStorageGc = StorageGarbageCollector.class.getDeclaredMethod(
                    "runStorageGC", long.class, Map.class, Map.class);
            runStorageGc.setAccessible(true);

            ReadPin gcRead = resources.getIngestReadPins().pin(ReadPin.newBuilder()
                    .setTransactionId(199).setReadTimestamp(201).build());
            runStorageGc.invoke(storageGc, 202L, fileStats, copyBitmaps(gcBitmaps));
            assertEquals(MetadataProto.File.Type.REGULAR, catalog.files.get(sourceFileId).getType(),
                    "A fixed ReadView must prevent the file-set switch");
            assertEquals(2, catalog.files.values().stream()
                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR).count());
            resources.getIngestReadPins().release(gcRead);

            runStorageGc.invoke(storageGc, 202L, fileStats, copyBitmaps(gcBitmaps));
            assertEquals(MetadataProto.File.Type.RETIRED, catalog.files.get(sourceFileId).getType());
            MetadataProto.File replacement = catalog.files.values().stream()
                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR)
                    .filter(f -> !originalRegularIds.contains(f.getId()))
                    .findFirst().orElseThrow(AssertionError::new);
            long replacementId = replacement.getId();
            String replacementPath = root.resolve("ordered")
                    .resolve(replacement.getName()).toUri().toString();
            assertEquals(sourceEntries.size() - deleteCount, countRows(replacementPath));
            for (IndexProto.PrimaryIndexEntry entry : sourceEntries) {
                IndexProto.RowLocation location = MainIndexFactory.Instance()
                        .getMainIndex(73).getLocation(entry.getRowId());
                assertNotNull(location);
                if (!deletedRowIds.contains(entry.getRowId())) {
                    assertEquals(replacementId, location.getFileId());
                }
            }

            Set<Long> checkpointFiles = new HashSet<>();
            catalog.files.values().stream()
                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR)
                    .forEach(f -> checkpointFiles.add(f.getId()));
            new StorageGcWal.RecoveryHandler(gcWal, MetadataService.Instance(), delegate)
                    .recover(checkpointFiles);
            Thread.sleep(2);
            resources.processRetiredFiles();
            assertFalse(catalog.files.containsKey(sourceFileId));
            assertFalse(StorageFactory.Instance().getStorage(sourcePath).exists(sourcePath));
            for (long deletedRowId : deletedRowIds) {
                assertNull(MainIndexFactory.Instance().getMainIndex(73).getLocation(deletedRowId));
            }
            MainIndexFactory.Instance().closeIndex(73, false);
            for (IndexProto.PrimaryIndexEntry entry : sourceEntries) {
                IndexProto.RowLocation location = MainIndexFactory.Instance()
                        .getMainIndex(73).getLocation(entry.getRowId());
                if (deletedRowIds.contains(entry.getRowId())) {
                    assertNull(location);
                } else {
                    assertNotNull(location);
                    assertEquals(replacementId, location.getFileId());
                }
            }
            List<String> terminalGcTasks = gcWal.listTerminalTasks().stream()
                    .map(StorageGcWal.Task::getTaskId).collect(java.util.stream.Collectors.toList());
            assertEquals(1, terminalGcTasks.size());
            gcWal.deleteTerminalTasks(terminalGcTasks);
            assertTrue(gcWal.listAllTasks().isEmpty(), "Checkpointed rewrite WAL must be removable");

            installer.close();
            installer = new PixelsIngestInstaller(
                    new InstallationStateStore(
                            planDirectory, INSTALLATION_STATE_BYTES,
                            INSTALLATION_COMPACTION_BYTES),
                    new IngestOptions(), "127.0.0.1:18890", resources, index,
                    MetadataService.Instance());
            Transaction publishedNext = next.toBuilder().setState(TransactionState.PUBLISHED)
                    .setProgress(PublicationProgress.VISIBLE_NOW)
                    .setCommitToken("storage-test-101").build();
            installer.initializeRecovery(Collections.singletonList(publishedNext));
            assertFalse(installer.recoveredByCheckpoint(published),
                    "Coordinator absence is the durable acknowledgement that prunes this checkpoint");

            long bufferedBeforeFile = bufferedRows(buffer);
            int lookupsBeforeFile = locationLookupCalls.get();
            long entryPutsBeforeFile = putCalls.get();
            long objectFilesBefore = countFiles(root.resolve("objects"));
            Set<Long> regularBeforeFile = catalog.files.values().stream()
                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR)
                    .map(MetadataProto.File::getId)
                    .collect(java.util.stream.Collectors.toSet());
            List<byte[][]> fileRows = Collections.nCopies(
                    200, new byte[][] {new byte[] {9}});
            MutationBatch fileBatch = new MutationBatch(
                    new MutationStreamId(
                            102, STATEMENT_ID, 1, 73, 0,
                            MutationStreamId.Kind.APPEND_ROWS),
                    0, table.getSchemaVersion(), ColumnBatchCodec.FORMAT, fileRows.size(),
                    ColumnBatchCodec.encode(fileRows, 1, 1024 * 1024));
            Transaction fileTransaction = tx.toBuilder()
                    .setTransactionId(102)
                    .setCommitTimestamp(203)
                    .setCommitToken("storage-test-file-102")
                    .setRepresentation(WriteRepresentation.FILE)
                    .build();
            AtomicInteger filePreparePasses = new AtomicInteger();
            installer.prepare(fileTransaction, () -> {
                assertEquals(1, filePreparePasses.incrementAndGet(),
                        "FILE Prepare must consume each WAL batch in one pass");
                return Collections.singletonList(fileBatch).iterator();
            });
            assertEquals(1, filePreparePasses.get());
            assertFalse(installer.install(
                    fileTransaction, Collections.singletonList(fileBatch), false, false));
            List<byte[][]> nextFileRows = Collections.nCopies(
                    20, new byte[][] {new byte[] {10}});
            MutationBatch nextFileBatch = new MutationBatch(
                    new MutationStreamId(
                            103, STATEMENT_ID, 1, 73, 0,
                            MutationStreamId.Kind.APPEND_ROWS),
                    0, table.getSchemaVersion(), ColumnBatchCodec.FORMAT, nextFileRows.size(),
                    ColumnBatchCodec.encode(nextFileRows, 1, 1024 * 1024));
            Transaction nextFileTransaction = fileTransaction.toBuilder()
                    .setTransactionId(103)
                    .setCommitTimestamp(204)
                    .setCommitToken("storage-test-file-103")
                    .build();
            installer.prepare(nextFileTransaction, Collections.singletonList(nextFileBatch));
            assertFalse(installer.install(
                    nextFileTransaction, Collections.singletonList(nextFileBatch), false, false));
            Iterable<MutationBatch> noPayloadReplay = () -> {
                throw new AssertionError("Publication polling must not reread installed WAL payloads");
            };
            assertFalse(installer.install(
                    fileTransaction, noPayloadReplay, false, false),
                    "Polling an earlier contribution must accept a later written file prefix");
            assertTrue(installer.install(
                    nextFileTransaction, noPayloadReplay, false, true));
            assertEquals(lookupsBeforeFile, locationLookupCalls.get(),
                    "Fresh FILE spans and completion polls must not look up individual rowIds");
            assertTrue(rangePutCalls.get() > 0, "Fresh FILE spans must use contiguous MainIndex ranges");
            assertEquals(entryPutsBeforeFile, putCalls.get(),
                    "Fresh FILE installation must not expand ranges into per-row index messages");
            assertEquals(bufferedBeforeFile, bufferedRows(buffer),
                    "FILE must not install rows into the shared MemTable");
            assertEquals(objectFilesBefore, countFiles(root.resolve("objects")),
                    "FILE must not create Retina object-staging blocks");
            List<MetadataProto.File> directFiles = catalog.files.values().stream()
                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR)
                    .filter(f -> !regularBeforeFile.contains(f.getId()))
                    .collect(java.util.stream.Collectors.toList());
            assertEquals(2, directFiles.size(),
                    "The row target and forced tail should produce two direct files");
            int directRows = 0;
            for (MetadataProto.File file : directFiles) {
                directRows += countRows(root.resolve("ordered")
                        .resolve(file.getName()).toUri().toString());
            }
            assertEquals(220, directRows);
        } finally {
            if (installer != null) {
                installer.close();
            }
            if (buffer != null) {
                buffer.close();
            }
            nodes.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            meta.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            previous.forEach(
                    (k, v) -> {
                        if (v != null) {
                            config.addProperty(k, v);
                        }
                    });
        }
    }

    private static Map<String, long[]> copyBitmaps(Map<String, long[]> source) {
        Map<String, long[]> copy = new HashMap<>();
        source.forEach((key, value) -> copy.put(key, value.clone()));
        return copy;
    }

    private static int countRows(String path) throws Exception {
        int rows = 0;
        try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(StorageFactory.Instance().getStorage(path))
                .setPath(path).setPixelsFooterCache(new PixelsFooterCache()).build()) {
            PixelsReaderOption option = new PixelsReaderOption();
            option.includeCols(new String[] {"v"});
            try (PixelsRecordReader records = reader.read(option)) {
                VectorizedRowBatch batch;
                while ((batch = records.readBatch()) != null && batch.size > 0) {
                    rows += batch.size;
                }
            }
        }
        return rows;
    }

    private static long countFiles(Path directory) throws Exception {
        if (!Files.exists(directory)) {
            return 0L;
        }
        try (java.util.stream.Stream<Path> paths = Files.walk(directory)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    static long readBuffered(RetinaResourceManager resources, Path root, long timestamp)
            throws Exception {
        RetinaProto.GetWriteBufferResponse response =
                resources.getWriteBuffer("s", "t", timestamp, 0).build();
        PixelsReaderOption option = new PixelsReaderOption();
        option.includeCols(new String[] {"v"});
        option.transTimestamp(timestamp);
        String folder = root.resolve("objects").toUri().toString();
        long count = 0;
        try (PixelsRecordReaderBufferImpl reader =
                new PixelsRecordReaderBufferImpl(
                        option,
                        io.pixelsdb.pixels.common.utils.NetUtils.getLocalHostName(),
                        response.getData().toByteArray(),
                        response.getIdsList(),
                        response.getBitmapsList(),
                        StorageFactory.Instance().getStorage(folder),
                        73,
                        0,
                        TypeDescription.fromString("struct<v:varbinary>"))) {
            while (!reader.isEndOfFile()) {
                VectorizedRowBatch batch = reader.readBatch();
                count += batch.size;
            }
        }
        return count;
    }

    static long bufferedRows(PixelsWriteBuffer buffer) {
        SuperVersion view = buffer.getCurrentVersion();
        try {
            long rows = view.getActiveMemTable() == null ? 0 : view.getActiveMemTable().getSize();
            for (MemTable mem : view.getImmutableMemTables()) {
                rows += mem.getSize();
            }
            for (ObjectEntry object : view.getObjectEntries()) {
                rows += object.getLength();
            }
            return rows;
        } finally {
            view.unref();
        }
    }

    static boolean hasFlushedFile(Path dir) throws Exception {
        try (java.util.stream.Stream<Path> paths = Files.walk(dir)) {
            for (Path path :
                    (Iterable<Path>)
                            paths.filter(
                                            p ->
                                                    p.toString().endsWith(".db")
                                                            || p.toString().endsWith(".sqlite"))
                                    ::iterator) {
                try (java.sql.Connection c =
                                java.sql.DriverManager.getConnection("jdbc:sqlite:" + path);
                        java.sql.Statement s = c.createStatement();
                        java.sql.ResultSet r =
                                s.executeQuery("select count(*) from row_id_range_flush_markers")) {
                    if (r.next() && r.getLong(1) > 0) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
