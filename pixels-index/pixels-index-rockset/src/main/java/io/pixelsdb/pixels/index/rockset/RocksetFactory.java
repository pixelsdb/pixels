package io.pixelsdb.pixels.index.rockset;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.rockset.jni.*;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RocksetFactory
{
    private static final RocksetFactoryStub stub = new RocksetFactoryStub();
    private static final String dbPath = ConfigFactory.Instance().getProperty("index.rockset.data.path");
    private static final boolean multiCF = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("index.rocksdb.multicf"));
    private static RocksetDB rocksetDB;

    private static RocksetCache blockCache;
    private static final long blockCacheCapacity = Long.parseLong(ConfigFactory.Instance().getProperty("index.rocksdb.block.cache.capacity"));
    private static final int blockCacheShardBits = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.block.cache.shard.bits"));
    /**
     * The reference counter.
     */
    private static final AtomicInteger reference = new AtomicInteger(0);
    private static final Map<String, RocksetColumnFamilyHandle> cfHandles = new ConcurrentHashMap<>();
    private static final String defaultColumnFamily = "default"; // Change for Rockset if needed

    private RocksetFactory() { }

    private static RocksetDB createRocksetDB()
    {
        // 1. Get existing column families (returns empty list for new database)
        List<byte[]> existingColumnFamilies;
        try
        {
            existingColumnFamilies = stub.listColumnFamilies0(dbPath);
        } catch (Exception e)
        {
            // For new database, return list containing only default column family
            existingColumnFamilies = Collections.singletonList(RocksDB.DEFAULT_COLUMN_FAMILY);
        }
        // 2. Ensure default column family is included
        if (!existingColumnFamilies.contains(RocksDB.DEFAULT_COLUMN_FAMILY))
        {
            existingColumnFamilies = new ArrayList<>(existingColumnFamilies);
            existingColumnFamilies.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        if (blockCache == null)
        {
            blockCache = new RocksetLRUCache(blockCacheCapacity, blockCacheShardBits);
        }

        // 3. Prepare column family descriptors
        List<RocksetColumnFamilyDescriptor> descriptors = existingColumnFamilies.stream()
                .map(RocksetFactory::createCFDescriptor)
                .collect(Collectors.toList());
        // 4. Open DB
        List<RocksetColumnFamilyHandle> handles = new ArrayList<>();
        int maxBackgroundFlushes = Integer.parseInt(ConfigFactory.Instance()
                        .getProperty("index.rockset.max.background.flushes"));

        int maxBackgroundCompactions = Integer.parseInt(ConfigFactory.Instance()
                        .getProperty("index.rockset.max.background.compactions"));

        int maxSubcompactions = Integer.parseInt(ConfigFactory.Instance()
                        .getProperty("index.rockset.max.subcompactions"));

        int maxOpenFiles = Integer.parseInt(ConfigFactory.Instance()
                        .getProperty("index.rockset.max.open.files"));

        boolean enableStats = Boolean.parseBoolean(ConfigFactory.Instance()
                        .getProperty("index.rockset.stats.enabled"));

        RocksetDBOptions options = RocksetDBOptions.create()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundFlushes(maxBackgroundFlushes)
                .setMaxBackgroundCompactions(maxBackgroundCompactions)
                .setMaxSubcompactions(maxSubcompactions)
                .setMaxOpenFiles(maxOpenFiles);

        if (enableStats)
        {
            String statsPath = ConfigFactory.Instance().getProperty("index.rockset.stats.path");
            int statsInterval = Integer.parseInt(ConfigFactory.Instance()
                            .getProperty("index.rockset.stats.interval"));
            RocksetStatistics statistics = new RocksetStatistics();
            options.setStatistics(statistics)
                    .setStatsDumpPeriodSec(statsInterval)
                    .setDbLogDir(statsPath);
        }

        RocksetDB db = RocksetDB.open(options, dbPath, descriptors, handles);

        // 5. Save handles for reuse
        for (int i = 0; i < descriptors.size(); i++)
        {
            String cfName = new String(descriptors.get(i).getName(), StandardCharsets.UTF_8);
            cfHandles.putIfAbsent(cfName, handles.get(i));
        }
        return db;
    }

    private static RocksetColumnFamilyDescriptor createCFDescriptor(byte[] name)
    {
        ConfigFactory config = ConfigFactory.Instance();

        long blockSize = Long.parseLong(config.getProperty("index.rocksdb.block.size"));
        RocksetBlockBasedTableConfig tableConfig = new RocksetBlockBasedTableConfig()
                .setFilterPolicy(new RocksetBloomFilter(10, false))
                .setWholeKeyFiltering(false)
                .setBlockSize(blockSize)
                .setBlockCache(blockCache);

        // ColumnFamily Options
        long writeBufferSize = Long.parseLong(config.getProperty("index.rocksdb.write.buffer.size"));
        int maxWriteBufferNumber = Integer.parseInt(config.getProperty("index.rocksdb.max.write.buffer.number"));
        int minWriteBufferNumberToMerge = Integer.parseInt(config.getProperty("index.rocksdb.min.write.buffer.number.to.merge"));

        // Compaction Options
        int level0FileNumCompactionTrigger = Integer.parseInt(config.getProperty("index.rocksdb.level0.file.num.compaction.trigger"));
        long maxBytesForLevelBase = Long.parseLong(config.getProperty("index.rocksdb.max.bytes.for.level.base"));
        int maxBytesForLevelMultiplier = Integer.parseInt(config.getProperty("index.rocksdb.max.bytes.for.level.multiplier"));
        long targetFileSizeBase = Long.parseLong(config.getProperty("index.rocksdb.target.file.size.base"));
        int targetFileSizeMultiplier = Integer.parseInt(config.getProperty("index.rocksdb.target.file.size.multiplier"));
        RocksetCompactionStyle compactionStyle = RocksetCompactionStyle.valueOf(config.getProperty("index.rocksdb.compaction.style"));

        // Compression Options
        RocksetCompressionType compressionType = RocksetCompressionType.valueOf(config.getProperty("index.rocksdb.compression.type"));
        RocksetCompressionType bottommostCompressionType = RocksetCompressionType.valueOf(config.getProperty("index.rocksdb.bottommost.compression.type"));

        RocksetColumnFamilyOptions cfOptions = new RocksetColumnFamilyOptions()
                .setWriteBufferSize(writeBufferSize)
                .setMaxWriteBufferNumber(maxWriteBufferNumber)
                .setMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge)
                .setMemtablePrefixBloomSizeRatio(0.1)
                .setTableFormatConfig(tableConfig)
                .setLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger)
                .setMaxBytesForLevelBase(maxBytesForLevelBase)
                .setMaxBytesForLevelMultiplier(maxBytesForLevelMultiplier)
                .setTargetFileSizeBase(targetFileSizeBase)
                .setTargetFileSizeMultiplier(targetFileSizeMultiplier)
                .setCompressionType(compressionType)
                .setBottommostCompressionType(bottommostCompressionType)
                .setCompactionStyle(compactionStyle);

        return new RocksetColumnFamilyDescriptor(name, cfOptions);
    }

    public static synchronized RocksetColumnFamilyHandle getOrCreateColumnFamily(long tableId, long indexId) throws Exception {
        String cfName = getCFName(tableId, indexId);

        // Return cached handle if exists
        if (cfHandles.containsKey(cfName)) {
            return cfHandles.get(cfName);
        }

        RocksetDB db = getRocksetDB();
        RocksetColumnFamilyHandle handle = stub.createColumnFamily(db.handle(), cfName.getBytes(StandardCharsets.UTF_8));
        cfHandles.put(cfName, handle);
        return handle;
    }

    private static String getCFName(long tableId, long indexId) {
        if(multiCF)
        {
            return "t" + tableId + "_i" + indexId;
        }
        else
        {
            return defaultColumnFamily;
        }
    }

    public static synchronized RocksetDB getRocksetDB() throws Exception {
        if (rocksetDB == null || rocksetDB.isClosed()) {
            rocksetDB = createRocksetDB();
        }
        reference.incrementAndGet();
        return rocksetDB;
    }

    public static synchronized void close() {
        if (rocksetDB != null && reference.decrementAndGet() == 0) {
            for (RocksetColumnFamilyHandle handle : cfHandles.values()) {
                handle.close(); // Ensure that native handles are properly closed
            }
            cfHandles.clear();
            // Add closing logic for your native database
            stub.closeNativeDatabase(rocksetDB);
            rocksetDB = null;
        }
    }

    public static synchronized String getDbPath() {
        return dbPath;
    }
}

