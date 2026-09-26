package io.pixelsdb.pixels.daemon.transaction.ingest;

import com.google.protobuf.Empty;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.error.ErrorCode;
import io.pixelsdb.pixels.common.index.service.*;
import io.pixelsdb.pixels.common.ingest.durable.AtomicStateFile;
import io.pixelsdb.pixels.common.ingest.rpc.*;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.NetUtils;
import io.pixelsdb.pixels.core.ingest.IngestTables;
import io.pixelsdb.pixels.daemon.*;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.ingest.IngestProto.*;
import io.pixelsdb.pixels.retina.*;
import io.pixelsdb.pixels.retina.ingest.*;

import java.io.*;
import java.lang.reflect.*;
import java.net.ServerSocket;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * SQL integration environment. Data, RPCs, journal, decisions, native visibility,
 * SQLite MainIndex, object spill, and Pixels files are real. The external catalog,
 * topology service, and etcd-backed identity sources are deterministic fixtures.
 */
public final class SqlIngestFixture implements AutoCloseable {
    private static final long SERVER_SHUTDOWN_TIMEOUT_SECONDS = 5L;
    private static final int DEFAULT_FIXTURE_ROUTE_COUNT = 1;

    private static String benchmarkSetting(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    private static int positiveBenchmarkInteger(String name, int fallback) {
        int value = Integer.parseInt(benchmarkSetting(name, Integer.toString(fallback)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static <T> void reply(StreamObserver<T> observer, T value) {
        observer.onNext(value);
        observer.onCompleted();
    }

    private static NodeProto.GetRetinaListResponse retinaNodes(
            String host, int port, int routeCount) {
        NodeProto.GetRetinaListResponse.Builder response =
                NodeProto.GetRetinaListResponse.newBuilder();
        for (int virtualNodeId = 0; virtualNodeId < routeCount; virtualNodeId++) {
            response.addNodes(
                    NodeProto.NodeInfo.newBuilder()
                            .setAddress(host)
                            .setPort(port)
                            .setVirtualNodeId(virtualNodeId));
        }
        return response.build();
    }

    private static MetadataProto.ResponseHeader ok(MetadataProto.RequestHeader request) {
        return MetadataProto.ResponseHeader.newBuilder().setToken(request.getToken()).build();
    }

    public static final class Catalog extends TestPixelsIngestStorage.Catalog
            implements AutoCloseable {
        private static final int CATALOG_MAGIC = 0x50464331;
        private static final int CATALOG_VERSION = 1;
        private static final int MAX_CATALOG_BYTES = 16 * 1024 * 1024;
        private static final long SCHEMA_ID = 1L;
        private static final long TABLE_T_ID = 73L;
        private static final long TABLE_A_ID = 74L;
        private static final long TABLE_B_ID = 75L;
        private static final long TABLE_T_LAYOUT_ID = 1L;
        private static final long TABLE_A_LAYOUT_ID = 2L;
        private static final long TABLE_B_LAYOUT_ID = 3L;
        private static final long TABLE_T_ORDERED_PATH_ID = 1L;
        private static final long TABLE_T_COMPACT_PATH_ID = 2L;
        private static final long TABLE_A_ORDERED_PATH_ID = 3L;
        private static final long TABLE_A_COMPACT_PATH_ID = 4L;
        private static final long TABLE_B_ORDERED_PATH_ID = 5L;
        private static final long TABLE_B_COMPACT_PATH_ID = 6L;
        private static final long ID_COLUMN_ID = 1L;
        private static final long LABEL_COLUMN_ID = 2L;
        private static final long INITIAL_CATALOG_ID = 100L;
        private static final int MAX_CATALOG_FILE_COUNT = 100_000;
        private static final String SCHEMA_NAME = "s";
        private static final String TABLE_T_NAME = "t";
        private static final String TABLE_A_NAME = "a";
        private static final String TABLE_B_NAME = "b";

        private final AtomicStateFile catalogState;
        private final Map<String, MetadataProto.Table> tables = new LinkedHashMap<>();
        private final Map<String, MetadataProto.Layout> layouts = new LinkedHashMap<>();

        Catalog(Path root) throws Exception {
            super(root);
            catalogState = new AtomicStateFile(root.resolve("fixture-catalog"), MAX_CATALOG_BYTES);
            restoreCatalog(catalogState.read());
            registerTable(root, TABLE_T_NAME, TABLE_T_ID, TABLE_T_LAYOUT_ID,
                    TABLE_T_ORDERED_PATH_ID, TABLE_T_COMPACT_PATH_ID);
            registerTable(root.resolve(TABLE_A_NAME), TABLE_A_NAME, TABLE_A_ID, TABLE_A_LAYOUT_ID,
                    TABLE_A_ORDERED_PATH_ID, TABLE_A_COMPACT_PATH_ID);
            registerTable(root.resolve(TABLE_B_NAME), TABLE_B_NAME, TABLE_B_ID, TABLE_B_LAYOUT_ID,
                    TABLE_B_ORDERED_PATH_ID, TABLE_B_COMPACT_PATH_ID);
        }

        private void registerTable(
                Path root, String name, long tableId, long layoutId,
                long orderedPathId, long compactPathId) throws IOException {
            Path ordered = Files.createDirectories(root.resolve("ordered"));
            Path compact = Files.createDirectories(root.resolve("compact"));
            MetadataProto.Table table = MetadataProto.Table.newBuilder()
                    .setId(tableId)
                    .setSchemaId(SCHEMA_ID)
                    .setName(name)
                    .setType("user")
                    .setStorageScheme("file")
                    .build();
            MetadataProto.Layout tableLayout = layout.toBuilder()
                    .setId(layoutId)
                    .setTableId(tableId)
                    .clearOrderedPaths()
                    .clearCompactPaths()
                    .addOrderedPaths(MetadataProto.Path.newBuilder()
                            .setId(orderedPathId)
                            .setLayoutId(layoutId)
                            .setUri(ordered.toUri().toString())
                            .setType(MetadataProto.Path.Type.ORDERED))
                    .addCompactPaths(MetadataProto.Path.newBuilder()
                            .setId(compactPathId)
                            .setLayoutId(layoutId)
                            .setUri(compact.toUri().toString())
                            .setType(MetadataProto.Path.Type.COMPACT))
                    .setOrdered("{\"columnOrder\":[\"id\",\"label\"]}")
                    .setSplits("{\"numRowGroupInFile\":1,\"splitPatterns\":[]}")
                    .build();
            tables.put(name, table);
            layouts.put(name, tableLayout);
        }

        private void restoreCatalog(byte[] snapshot) throws Exception {
            if (snapshot.length == 0) {
                return;
            }
            try (DataInputStream input =
                    new DataInputStream(new ByteArrayInputStream(snapshot))) {
                if (input.readInt() != CATALOG_MAGIC
                        || input.readInt() != CATALOG_VERSION) {
                    throw new IOException("Invalid fixture catalog state header");
                }
                long restoredId = input.readLong();
                int count = input.readInt();
                if (restoredId < INITIAL_CATALOG_ID
                        || count < 0
                        || count > MAX_CATALOG_FILE_COUNT) {
                    throw new IOException("Invalid fixture catalog state bounds");
                }
                long maximumId = INITIAL_CATALOG_ID;
                for (int i = 0; i < count; i++) {
                    int length = input.readInt();
                    if (length <= 0 || length > MAX_CATALOG_BYTES || length > input.available()) {
                        throw new IOException("Invalid fixture catalog file entry length");
                    }
                    byte[] bytes = new byte[length];
                    input.readFully(bytes);
                    MetadataProto.File file = MetadataProto.File.parseFrom(bytes);
                    if (file.getId() <= 0 || files.put(file.getId(), file) != null) {
                        throw new IOException("Invalid duplicate fixture catalog file identity");
                    }
                    maximumId = Math.max(maximumId, file.getId());
                }
                if (input.available() != 0 || restoredId < maximumId) {
                    throw new IOException("Invalid trailing fixture catalog state");
                }
                ids.set(restoredId);
            }
        }

        private byte[] snapshotCatalog() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(CATALOG_MAGIC);
                output.writeInt(CATALOG_VERSION);
                output.writeLong(ids.get());
                List<MetadataProto.File> ordered = new ArrayList<>(files.values());
                ordered.sort(Comparator.comparingLong(MetadataProto.File::getId));
                output.writeInt(ordered.size());
                for (MetadataProto.File file : ordered) {
                    byte[] encoded = file.toByteArray();
                    output.writeInt(encoded.length);
                    output.write(encoded);
                }
            }
            return bytes.toByteArray();
        }

        @Override
        protected synchronized void catalogMutated() {
            try {
                catalogState.store(snapshotCatalog());
            } catch (IOException failure) {
                throw new UncheckedIOException("Cannot persist fixture catalog mutation", failure);
            }
        }

        @Override
        public void close() throws IOException {
            catalogState.close();
        }

        @Override
        public void getTable(
                MetadataProto.GetTableRequest r, StreamObserver<MetadataProto.GetTableResponse> o) {
            MetadataProto.Table table = requireTable(r.getTableName());
            reply(
                    o,
                    MetadataProto.GetTableResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setTable(table)
                            .addLayouts(layouts.get(table.getName()))
                            .build());
        }

        @Override
        public void getColumns(
                MetadataProto.GetColumnsRequest r,
                StreamObserver<MetadataProto.GetColumnsResponse> o) {
            MetadataProto.Table table = requireTable(r.getTableName());
            reply(
                    o,
                    MetadataProto.GetColumnsResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .addColumns(
                                    MetadataProto.Column.newBuilder()
                                            .setId(ID_COLUMN_ID)
                                            .setTableId(table.getId())
                                            .setName("id")
                                            .setType("bigint"))
                            .addColumns(
                                    MetadataProto.Column.newBuilder()
                                            .setId(LABEL_COLUMN_ID)
                                            .setTableId(table.getId())
                                            .setName("label")
                                            .setType("varchar"))
                            .build());
        }

        @Override
        public void getLayout(
                MetadataProto.GetLayoutRequest r,
                StreamObserver<MetadataProto.GetLayoutResponse> o) {
            reply(
                    o,
                    MetadataProto.GetLayoutResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setLayout(requireLayout(r.getTableName()))
                            .build());
        }

        @Override
        public void getLayouts(
                MetadataProto.GetLayoutsRequest r,
                StreamObserver<MetadataProto.GetLayoutsResponse> o) {
            reply(
                    o,
                    MetadataProto.GetLayoutsResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .addLayouts(requireLayout(r.getTableName()))
                            .build());
        }

        @Override
        public void getSchemas(
                MetadataProto.GetSchemasRequest r,
                StreamObserver<MetadataProto.GetSchemasResponse> o) {
            reply(
                    o,
                    MetadataProto.GetSchemasResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .addSchemas(MetadataProto.Schema.newBuilder()
                                    .setId(SCHEMA_ID).setName(SCHEMA_NAME))
                            .build());
        }

        @Override
        public void getTables(
                MetadataProto.GetTablesRequest r,
                StreamObserver<MetadataProto.GetTablesResponse> o) {
            reply(
                    o,
                    MetadataProto.GetTablesResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .addAllTables(tables.values())
                            .build());
        }

        @Override
        public void existSchema(
                MetadataProto.ExistSchemaRequest r,
                StreamObserver<MetadataProto.ExistSchemaResponse> o) {
            reply(
                    o,
                    MetadataProto.ExistSchemaResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setExists(r.getSchemaName().equals(SCHEMA_NAME))
                            .build());
        }

        @Override
        public void existTable(
                MetadataProto.ExistTableRequest r,
                StreamObserver<MetadataProto.ExistTableResponse> o) {
            reply(
                    o,
                    MetadataProto.ExistTableResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setExists(r.getSchemaName().equals(SCHEMA_NAME)
                                    && tables.containsKey(r.getTableName()))
                            .build());
        }

        @Override
        public void getTableById(
                MetadataProto.GetTableByIdRequest r,
                StreamObserver<MetadataProto.GetTableByIdResponse> o) {
            MetadataProto.Table table = tables.values().stream()
                    .filter(value -> value.getId() == r.getTableId())
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown fixture table id"));
            reply(
                    o,
                    MetadataProto.GetTableByIdResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setTable(table)
                            .build());
        }

        private MetadataProto.Table requireTable(String name) {
            MetadataProto.Table table = tables.get(name);
            if (table == null) {
                throw new IllegalArgumentException("Unknown fixture table: " + name);
            }
            return table;
        }

        private MetadataProto.Layout requireLayout(String name) {
            MetadataProto.Layout tableLayout = layouts.get(name);
            if (tableLayout == null) {
                throw new IllegalArgumentException("Unknown fixture table layout: " + name);
            }
            return tableLayout;
        }

        @Override
        public void getView(
                MetadataProto.GetViewRequest r, StreamObserver<MetadataProto.GetViewResponse> o) {
            reply(
                    o,
                    MetadataProto.GetViewResponse.newBuilder()
                            .setHeader(
                                    ok(r.getHeader()).toBuilder()
                                            .setErrorCode(ErrorCode.METADATA_VIEW_NOT_FOUND))
                            .build());
        }

        @Override
        public void existView(
                MetadataProto.ExistViewRequest r,
                StreamObserver<MetadataProto.ExistViewResponse> o) {
            reply(
                    o,
                    MetadataProto.ExistViewResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setExists(false)
                            .build());
        }

        @Override
        public void getViews(
                MetadataProto.GetViewsRequest r, StreamObserver<MetadataProto.GetViewsResponse> o) {
            reply(
                    o,
                    MetadataProto.GetViewsResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .build());
        }

        @Override
        public void getFileType(
                MetadataProto.GetFileTypeRequest r,
                StreamObserver<MetadataProto.GetFileTypeResponse> o) {
            MetadataProto.File file =
                    files.values().stream()
                            .filter(f -> r.getFilePathUri().endsWith("/" + f.getName()))
                            .findFirst()
                            .orElseThrow(IllegalArgumentException::new);
            reply(
                    o,
                    MetadataProto.GetFileTypeResponse.newBuilder()
                            .setHeader(ok(r.getHeader()))
                            .setFileType(file.getType())
                            .build());
        }

        public long publishedFileCount() {
            return files.values().stream()
                    .filter(f -> f.getType() == MetadataProto.File.Type.REGULAR)
                    .count();
        }

        public long publishedFileCount(String tableName) {
            MetadataProto.Layout tableLayout = requireLayout(tableName);
            Set<Long> pathIds = new HashSet<>();
            tableLayout.getOrderedPathsList().forEach(path -> pathIds.add(path.getId()));
            tableLayout.getCompactPathsList().forEach(path -> pathIds.add(path.getId()));
            return files.values().stream()
                    .filter(file -> file.getType() == MetadataProto.File.Type.REGULAR)
                    .filter(file -> pathIds.contains(file.getPathId()))
                    .count();
        }
    }

    public final Path root;
    public final Catalog catalog;
    public final RetinaResourceManager resources;
    public final DurableIngestCoordinator coordinator;
    public final IngestClient client;
    private final Server metadataServer, nodeServer, transactionServer, retinaServer;
    private final RetinaIngestParticipant participant;
    private final String owner;
    private final boolean recoveryCheckpointEnabled;
    private final Map<String, String> exportedSettings = new LinkedHashMap<>();
    public final AtomicLong appendRpcCount = new AtomicLong();
    public final AtomicLong acceptedRows = new AtomicLong();
    public final AtomicLong bufferReadRpcCount = new AtomicLong();
    public final AtomicLong fileReadRpcCount = new AtomicLong();

    public SqlIngestFixture() throws Exception {
        root = Files.createTempDirectory("pixels-full-sql-");
        Files.createDirectories(root.resolve("ordered"));
        Files.createDirectories(root.resolve("compact"));
        String secret = "sql-fixture-" + UUID.randomUUID();
        Path secretFile = root.resolve("credential");
        Files.write(secretFile, secret.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String host = NetUtils.getLocalHostName();
        int retinaPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            retinaPort = socket.getLocalPort();
        }
        owner = host + ":" + retinaPort;
        recoveryCheckpointEnabled = Boolean.parseBoolean(
                benchmarkSetting("PIXELS_SQL_FIXTURE_RECOVERY_CHECKPOINT", "false"));
        int routeCount = positiveBenchmarkInteger(
                "PIXELS_SQL_FIXTURE_ROUTE_COUNT", DEFAULT_FIXTURE_ROUTE_COUNT);
        ConfigFactory config = ConfigFactory.Instance();
        Map<String, String> settings = exportedSettings;
        settings.put("retina.enable", "true");
        settings.put("retina.ingest.enabled", "true");
        settings.put("retina.ingest.auth.secret.file", secretFile.toString());
        settings.put("retina.ingest.coordinator.state.dir", root.resolve("decisions").toString());
        settings.put("retina.ingest.participant.plan.dir", root.resolve("plans").toString());
        settings.put("retina.ingest.participant.wal.dir", root.resolve("wal").toString());
        settings.put(
                "retina.ingest.commit.ack",
                benchmarkSetting("PIXELS_SQL_FIXTURE_COMMIT_ACK", "VISIBLE"));
        settings.put(
                "retina.ingest.write.representation",
                benchmarkSetting("PIXELS_SQL_FIXTURE_REPRESENTATION", "BUFFERED"));
        settings.put(
                "retina.ingest.file.target.rows",
                benchmarkSetting("PIXELS_SQL_FIXTURE_FILE_TARGET_ROWS", "1000000"));
        settings.put(
                "retina.ingest.file.max.bytes",
                benchmarkSetting("PIXELS_SQL_FIXTURE_FILE_MAX_BYTES", "536870912"));
        settings.put(
                "retina.ingest.file.max.delay.ms",
                benchmarkSetting("PIXELS_SQL_FIXTURE_FILE_MAX_DELAY_MS", "30000"));
        settings.put(
                "retina.ingest.file.poll.ms",
                benchmarkSetting("PIXELS_SQL_FIXTURE_FILE_POLL_MS", "25"));
        settings.put("retina.storage.gc.enabled", "false");
        if (recoveryCheckpointEnabled) {
            settings.put("retina.gc.interval", "1");
            settings.put("etcd.hosts", "127.0.0.1");
            settings.put(
                    "etcd.port",
                    benchmarkSetting("PIXELS_SQL_FIXTURE_ETCD_PORT", "2379"));
            settings.put(
                    "retina.recovery.checkpoint.dir",
                    root.resolve("recovery").toUri().toString());
        }
        settings.put(
                "retina.buffer.memTable.size",
                benchmarkSetting("PIXELS_SQL_FIXTURE_MEMTABLE_ROWS", "64"));
        settings.put(
                "retina.buffer.flush.count",
                benchmarkSetting("PIXELS_SQL_FIXTURE_FLUSH_COUNT", "2"));
        settings.put(
                "retina.buffer.flush.interval",
                benchmarkSetting("PIXELS_SQL_FIXTURE_FLUSH_INTERVAL_SECONDS", "1"));
        settings.put(
                "retina.buffer.object.flush.threads",
                benchmarkSetting("PIXELS_SQL_FIXTURE_FILE_FLUSH_THREADS", "4"));
        settings.put(
                "retina.buffer.object.storage.folder", root.resolve("objects").toUri().toString());
        settings.put("retina.storage.gc.journal.dir", root.resolve("gc").toUri().toString());
        settings.put("retina.offload.checkpoint.dir", root.resolve("offload").toUri().toString());
        settings.put("index.sqlite.path", root.resolve("sqlite").toString());
        settings.put("enabled.storage.schemes", "file");
        settings.put("node.bucket.num", Integer.toString(routeCount));
        settings.put("node.virtual.num", Integer.toString(routeCount));
        settings.put("index.bucket.num", "1");
        settings.put("index.cache.enabled", "false");
        settings.put("cache.enabled", "false");
        settings.put("projection.read.enabled", "false");
        settings.put("fixed.split.size", "1");
        settings.put("scaling.enabled", "false");
        settings.put("retina.buffer.split.enable", "true");
        settings.put("retina.server.host", host);
        settings.put("retina.server.port", Integer.toString(retinaPort));
        settings.put(
                "retina.ingest.max.batch.rows",
                benchmarkSetting("PIXELS_SQL_FIXTURE_MAX_BATCH_ROWS", "32"));
        settings.put(
                "retina.ingest.max.batch.bytes",
                benchmarkSetting("PIXELS_SQL_FIXTURE_MAX_BATCH_BYTES", "4096"));
        settings.put(
                "retina.ingest.max.state.bytes",
                benchmarkSetting("PIXELS_SQL_FIXTURE_MAX_STATE_BYTES", "67108864"));
        settings.put(
                "retina.ingest.max.prepared.rows",
                benchmarkSetting("PIXELS_SQL_FIXTURE_MAX_PREPARED_ROWS", "1000000"));
        settings.put(
                "retina.ingest.install.rpc.timeout.ms",
                benchmarkSetting("PIXELS_SQL_FIXTURE_INSTALL_RPC_TIMEOUT_MS", "3600000"));
        settings.put(
                "retina.ingest.wal.max.bytes",
                benchmarkSetting("PIXELS_SQL_FIXTURE_WAL_MAX_BYTES", "4294967296"));
        settings.put(
                "retina.ingest.wal.max.records",
                benchmarkSetting("PIXELS_SQL_FIXTURE_WAL_MAX_RECORDS", "10000000"));
        settings.put(
                "retina.ingest.wal.group.commit.delay.micros",
                benchmarkSetting(
                        "PIXELS_SQL_FIXTURE_WAL_GROUP_COMMIT_DELAY_MICROS", "200"));
        settings.put(
                "retina.ingest.wal.read.cache.max.bytes",
                benchmarkSetting(
                        "PIXELS_SQL_FIXTURE_WAL_READ_CACHE_MAX_BYTES", "67108864"));
        settings.put(
                "retina.ingest.coordinator.group.commit.delay.micros",
                benchmarkSetting(
                        "PIXELS_SQL_FIXTURE_COORDINATOR_GROUP_COMMIT_DELAY_MICROS", "200"));
        settings.forEach(config::addProperty);
        catalog = new Catalog(root);
        metadataServer = ServerBuilder.forPort(0).addService(catalog).build().start();
        config.addProperty("metadata.server.host", "127.0.0.1");
        config.addProperty("metadata.server.port", Integer.toString(metadataServer.getPort()));
        nodeServer =
                ServerBuilder.forPort(0)
                        .addService(
                                new NodeServiceGrpc.NodeServiceImplBase() {
                                    @Override
                                    public void getRetinaByBucket(
                                            NodeProto.GetRetinaByBucketRequest r,
                                            StreamObserver<NodeProto.GetRetinaByBucketResponse> o) {
                                        reply(
                                                o,
                                                NodeProto.GetRetinaByBucketResponse.newBuilder()
                                                        .setNode(NodeProto.NodeInfo.newBuilder()
                                                                .setAddress(host)
                                                                .setPort(retinaPort)
                                                                .setVirtualNodeId(r.getBucket()))
                                                        .build());
                                    }

                                    @Override
                                    public void getRetinaList(
                                            Empty r,
                                            StreamObserver<NodeProto.GetRetinaListResponse> o) {
                                        reply(
                                                o,
                                                retinaNodes(
                                                        host, retinaPort, routeCount));
                                    }
                                })
                        .build()
                        .start();
        config.addProperty("node.server.host", "127.0.0.1");
        config.addProperty("node.server.port", Integer.toString(nodeServer.getPort()));
        AtomicLong identities = new AtomicLong(10000);
        IngestOptions ingestOptions = new IngestOptions();
        AtomicReference<IngestClient> clients = new AtomicReference<>();
        coordinator =
                new DurableIngestCoordinator(
                        new CoordinatorStateStore(root.resolve("decisions"),
                                ingestOptions.maxStateBytes,
                                ingestOptions.coordinatorCompactionBytes),
                        new DurableIngestCoordinator.Tables() {
                            public TableSpec load(String s, String t) throws Exception {
                                return IngestTables.load(s, t);
                            }

                            public List<Route> routes() throws Exception {
                                return IngestTables.routes();
                            }
                        },
                        new DurableIngestCoordinator.Participants() {
                            public PrepareToken prepare(String o, Transaction tx) {
                                return clients.get()
                                        .participant(o)
                                        .prepare(
                                                ParticipantRequest.newBuilder()
                                                        .setOwner(o)
                                                        .setTransaction(tx)
                                                        .build());
                            }

                            public boolean install(
                                    String o, Transaction tx, boolean forceFileTail) {
                                return clients.get()
                                        .participant(
                                                o,
                                                ingestOptions.installRpcTimeoutMillis,
                                                TimeUnit.MILLISECONDS)
                                        .install(
                                                ParticipantRequest.newBuilder()
                                                        .setOwner(o)
                                                        .setTransaction(tx)
                                                        .setForceFileTail(forceFileTail)
                                                        .build())
                                        .getReady();
                            }

                            public void checkpoint(String o, long tx) {
                                clients.get().participant(o).checkpoint(IngestWire.id(tx));
                            }

                            public void discard(String o, long tx) {
                                clients.get().participant(o).discard(IngestWire.id(tx));
                            }
                        },
                        identities::incrementAndGet,
                        Clock.systemUTC(),
                        0,
                        ingestOptions.transactionLeaseMillis,
                        ingestOptions.maxTransactions,
                        ingestOptions.maxStreams,
                        ingestOptions.terminalRetentionMillis,
                        ingestOptions.maxTerminalTransactions,
                        ingestOptions.installationThreads,
                        ingestOptions.coordinatorGroupCommitDelayMicros);
        // Only the existing read-ID service is a fixture. Write outcomes use the real durable
        // coordinator above.
        TransServiceGrpc.TransServiceImplBase readTransactions =
                new TransServiceGrpc.TransServiceImplBase() {
                    @Override
                    public void beginTrans(
                            TransProto.BeginTransRequest r,
                            StreamObserver<TransProto.BeginTransResponse> o) {
                        reply(
                                o,
                                TransProto.BeginTransResponse.newBuilder()
                                        .setTransId(identities.incrementAndGet())
                                        .setTimestamp(coordinator.publishedTimestamp())
                                        .build());
                    }

                    @Override
                    public void commitTrans(
                            TransProto.CommitTransRequest r,
                            StreamObserver<TransProto.CommitTransResponse> o) {
                        reply(o, TransProto.CommitTransResponse.getDefaultInstance());
                    }

                    @Override
                    public void rollbackTrans(
                            TransProto.RollbackTransRequest r,
                            StreamObserver<TransProto.RollbackTransResponse> o) {
                        reply(o, TransProto.RollbackTransResponse.getDefaultInstance());
                    }

                    @Override
                    public void getSafeVisibilityFoldingTimestamp(
                            TransProto.GetSafeVisibilityFoldingTimestampRequest r,
                            StreamObserver<TransProto.GetSafeVisibilityFoldingTimestampResponse> o) {
                        reply(
                                o,
                                TransProto.GetSafeVisibilityFoldingTimestampResponse.newBuilder()
                                        .setErrorCode(ErrorCode.SUCCESS)
                                        .setTimestamp(coordinator.publishedTimestamp())
                                        .build());
                    }
                };
        transactionServer =
                ServerBuilder.forPort(0)
                        .addService(readTransactions)
                        .addService(
                                ServerInterceptors.intercept(
                                        new IngestCoordinatorRpc(coordinator),
                                        IngestAuth.server(secret)))
                        .build()
                        .start();
        config.addProperty("trans.server.host", "127.0.0.1");
        config.addProperty("trans.server.port", Integer.toString(transactionServer.getPort()));
        config.addProperty("retina.ingest.coordinator.host", "127.0.0.1");
        config.addProperty(
                "retina.ingest.coordinator.port", Integer.toString(transactionServer.getPort()));
        client =
                new IngestClient(
                        "127.0.0.1",
                        transactionServer.getPort(),
                        secret,
                        ingestOptions.maxStateBytes,
                        ingestOptions.transactionLeaseMillis);
        clients.set(client);
        resources = RetinaResourceManager.Instance();
        for (String tableName : Arrays.asList(
                Catalog.TABLE_T_NAME, Catalog.TABLE_A_NAME, Catalog.TABLE_B_NAME)) {
            resources.addWriteBuffer(Catalog.SCHEMA_NAME, tableName);
            resources.getIngestBuffer(Catalog.SCHEMA_NAME, tableName, 0);
        }
        AtomicLong rowIds = new AtomicLong(1000);
        IndexService actualIndexes = LocalIndexService.Instance();
        IndexService index =
                (IndexService)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {IndexService.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("allocateRowIdBatch")) {
                                        int n = (Integer) args[1];
                                        return IndexProto.RowIdBatch.newBuilder()
                                                .setRowIdStart(rowIds.getAndAdd(n))
                                                .setLength(n)
                                                .build();
                                    }
                                    try {
                                        return method.invoke(actualIndexes, args);
                                    } catch (InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                });
        PixelsIngestInstaller installer =
                new PixelsIngestInstaller(
                        new InstallationStateStore(
                                root.resolve("plans"), ingestOptions.maxStateBytes,
                                ingestOptions.planCompactionBytes),
                        ingestOptions,
                        owner,
                        resources,
                        index,
                        MetadataService.Instance());
        participant =
                new RetinaIngestParticipant(
                        owner,
                        new LocalMutationJournal(
                                Files.createDirectories(root.resolve("wal")),
                                ingestOptions.walSegmentBytes,
                                ingestOptions.walMaxBytes,
                                ingestOptions.walMaxRecords,
                                ingestOptions.walGroupCommitDelayMicros,
                                ingestOptions.walReadCacheMaxBytes),
                        new RetinaIngestParticipant.Decisions() {
                            public Transaction get(long id) {
                                return client.coordinator().getWrite(IngestWire.id(id));
                            }

                            public Transaction abort(long id) {
                                return client.coordinator().abortWrite(IngestWire.id(id));
                            }

                            public TransactionList list(String o) {
                                return client.coordinator()
                                        .listWrites(OwnerRequest.newBuilder().setOwner(o).build());
                            }
                        },
                        installer,
                        resources.getIngestReadPins());
        RetinaWorkerServiceGrpc.RetinaWorkerServiceImplBase reads =
                new RetinaWorkerServiceGrpc.RetinaWorkerServiceImplBase() {
                    @Override
                    public void getWriteBuffer(
                            RetinaProto.GetWriteBufferRequest r,
                            StreamObserver<RetinaProto.GetWriteBufferResponse> o) {
                        try {
                            bufferReadRpcCount.incrementAndGet();
                            reply(
                                    o,
                                    resources
                                            .getWriteBuffer(
                                                    r.getSchemaName(),
                                                    r.getTableName(),
                                                    r.getTimestamp(),
                                                    r.getVirtualNodeId())
                                            .setHeader(
                                                    RetinaProto.ResponseHeader.newBuilder()
                                                            .setToken(r.getHeader().getToken()))
                                            .build());
                        } catch (Exception e) {
                            o.onError(
                                    Status.INTERNAL
                                            .withDescription(e.toString())
                                            .asRuntimeException());
                        }
                    }

                    @Override
                    public void queryVisibility(
                            RetinaProto.QueryVisibilityRequest r,
                            StreamObserver<RetinaProto.QueryVisibilityResponse> o) {
                        try {
                            fileReadRpcCount.incrementAndGet();
                            RetinaProto.QueryVisibilityResponse.Builder b =
                                    RetinaProto.QueryVisibilityResponse.newBuilder()
                                            .setHeader(
                                                    RetinaProto.ResponseHeader.newBuilder()
                                                            .setToken(r.getHeader().getToken()));
                            for (int rg : r.getRgIdsList()) {
                                RetinaProto.VisibilityBitmap.Builder bitmap =
                                        RetinaProto.VisibilityBitmap.newBuilder();
                                for (long word :
                                        resources.queryVisibility(
                                                r.getFileId(), rg, r.getTimestamp())) {
                                    bitmap.addBitmap(word);
                                }
                                b.addBitmaps(bitmap);
                            }
                            reply(o, b.build());
                        } catch (Exception e) {
                            o.onError(
                                    Status.INTERNAL
                                            .withDescription(e.toString())
                                            .asRuntimeException());
                        }
                    }
                };
        ServerInterceptor countAppends =
                new ServerInterceptor() {
                    @Override
                    public <Q, A> ServerCall.Listener<Q> interceptCall(
                            ServerCall<Q, A> call, Metadata h, ServerCallHandler<Q, A> next) {
                        if (!call.getMethodDescriptor().getFullMethodName().endsWith("/Append")) {
                            return next.startCall(call, h);
                        }
                        appendRpcCount.incrementAndGet();
                        AtomicLong rows = new AtomicLong();
                        ServerCall<Q, A> tracked =
                                new ForwardingServerCall.SimpleForwardingServerCall<Q, A>(call) {
                                    @Override
                                    public void close(Status status, Metadata trailers) {
                                        if (status.isOk()) {
                                            acceptedRows.addAndGet(rows.get());
                                        }
                                        super.close(status, trailers);
                                    }
                                };
                        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<
                                Q>(next.startCall(tracked, h)) {
                            @Override
                            public void onMessage(Q message) {
                                if (message instanceof AppendRequest) {
                                    rows.set(((AppendRequest) message).getRowCount());
                                }
                                super.onMessage(message);
                            }
                        };
                    }
                };
        retinaServer =
                ServerBuilder.forPort(retinaPort)
                        .addService(reads)
                        .addService(
                                ServerInterceptors.intercept(
                                        new IngestParticipantRpc(participant),
                                        countAppends,
                                        IngestAuth.server(secret)))
                        .build()
                        .start();
        participant.recover();
        coordinator.start();
        if (recoveryCheckpointEnabled) {
            resources.startBackgroundGc();
        }
        for (String key :
                Arrays.asList(
                        "metadata.server.host",
                        "metadata.server.port",
                        "node.server.host",
                        "node.server.port",
                        "trans.server.host",
                        "trans.server.port",
                        "retina.ingest.coordinator.host",
                        "retina.ingest.coordinator.port")) {
            exportedSettings.put(key, config.getProperty(key));
        }
    }

    /** Export deployment settings, not runtime objects, for a separate Trino process. */
    public void exportConfiguration(Path destination) throws Exception {
        Properties properties = new Properties();
        try (java.io.InputStream input =
                ConfigFactory.class.getResourceAsStream("/pixels.properties")) {
            properties.load(input);
        }
        for (String key : new ArrayList<>(properties.stringPropertyNames())) {
            String value = ConfigFactory.Instance().getProperty(key);
            if (value != null) {
                properties.setProperty(key, value);
            }
        }
        exportedSettings.forEach(properties::setProperty);
        try (java.io.OutputStream output = Files.newOutputStream(destination)) {
            properties.store(output, "Local SQL integration fixture");
        }
    }

    public void exportStatus(Path destination) throws Exception {
        Properties status = new Properties();
        status.setProperty("dataRoot", root.toString());
        status.setProperty("pixelsFiles", Long.toString(catalog.publishedFileCount()));
        status.setProperty("primaryTablePixelsFiles",
                Long.toString(catalog.publishedFileCount(Catalog.TABLE_T_NAME)));
        status.setProperty("abortedTransactions", Long.toString(abortedTransactions()));
        status.setProperty(
                "activeTransactions",
                Integer.toString(coordinator.list(owner).getTransactionsCount()));
        status.setProperty("appendRPCs", Long.toString(appendRpcCount.get()));
        status.setProperty("acceptedRows", Long.toString(acceptedRows.get()));
        status.setProperty("bufferReadRPCs", Long.toString(bufferReadRpcCount.get()));
        status.setProperty("fileVisibilityRPCs", Long.toString(fileReadRpcCount.get()));
        Path temporary = destination.resolveSibling(destination.getFileName() + ".new");
        try (java.io.OutputStream output = Files.newOutputStream(temporary)) {
            status.store(output, "Integration counters");
        }
        Files.move(
                temporary,
                destination,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    public long abortedTransactions() throws Exception {
        return coordinator.list(owner).getTransactionsList().stream()
                .filter(t -> t.getState() == TransactionState.ABORTED)
                .count();
    }

    @Override
    public void close() throws Exception {
        coordinator.close();
        participant.close();
        // The resource manager owns both public BUFFERED buffers and the isolated FILE buffers
        // created on demand. Closing only the buffers known during fixture bootstrap would leave
        // FILE writers running after the SQL client exits.
        resources.shutdown();
        client.close();
        List<Server> servers =
                Arrays.asList(retinaServer, transactionServer, nodeServer, metadataServer);
        for (Server server : servers) {
            server.shutdownNow();
        }
        for (Server server : servers) {
            server.awaitTermination(SERVER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
        catalog.close();
    }
}
