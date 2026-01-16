/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.pixelsdb.pixels.index.rocksdb;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.core.TypeDescription;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @package: io.pixelsdb.pixels.index.rocksdb
 * @className: RocksDBFactory
 * @author: AntiO2
 * @date: 2025/9/8 10:33
 */
public class RocksDBFactory
{
    private static final Logger logger = LoggerFactory.getLogger(RocksDBFactory.class);
    private static final String dbPath = ConfigFactory.Instance().getProperty("index.rocksdb.data.path");
    private static final boolean multiCF = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("index.rocksdb.multicf"));
    private static RocksDB instance;

    private static Cache blockCache;
    private static final long blockCacheCapacity = Long.parseLong(ConfigFactory.Instance().getProperty("index.rocksdb.block.cache.capacity"));
    private static final int blockCacheShardBits = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.block.cache.shard.bits"));

    /**
     * The reference counter.
     */
    private static final AtomicInteger reference = new AtomicInteger(0);
    private static final Map<String, ColumnFamilyHandle> cfHandles = new ConcurrentHashMap<>();
    private static final String defaultColumnFamily = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);
    private static final Map<Long, Integer> indexKeyLenCache = new ConcurrentHashMap<>();
    private static final Integer VARIABLE_LEN_SENTINEL = -2;
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
    {
        Thread thread = new Thread(runnable, "RocksDB-Metrics-Logger");
        thread.setDaemon(true);
        return thread;
    });

    private RocksDBFactory() { }

    static RocksDB createRocksDB(String rocksDBPath) throws RocksDBException
    {
        // 1. Get existing column families (returns empty list for new database)
        List<byte[]> existingColumnFamilies;
        try
        {
            existingColumnFamilies = RocksDB.listColumnFamilies(new Options(), rocksDBPath);
        } catch (RocksDBException e)
        {
            // For new database, return list containing only default column family
            existingColumnFamilies = Collections.singletonList(RocksDB.DEFAULT_COLUMN_FAMILY);
        }
        // 2. Ensure default column family is included
        boolean existDefaultCF = false;
        for (byte[] cf : existingColumnFamilies)
        {
            if (Arrays.equals(cf, RocksDB.DEFAULT_COLUMN_FAMILY))
            {
                existDefaultCF = true;
                break;
            }
        }
        if (!existDefaultCF)
        {
            existingColumnFamilies = new ArrayList<>(existingColumnFamilies);
            existingColumnFamilies.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        if (blockCache == null)
        {
            blockCache = new LRUCache(blockCacheCapacity, blockCacheShardBits);
        }

        // 3. Prepare column family descriptors
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        for(byte[] existingColumnFamily: existingColumnFamilies)
        {
            long[] ids = parseTableAndIndexId(existingColumnFamily);
            Integer keyLen = null;
            if(ids != null)
            {
                long tableId = ids[0];
                long indexId = ids[1];
                try
                {
                    keyLen = getIndexKeyLen(tableId, indexId);
                } catch (MetadataException ignored)
                {

                }
            }
            descriptors.add(createCFDescriptor(existingColumnFamily, keyLen));
        }

        // 4. Open DB
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        int maxBackgroundFlushed = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.max.background.flushes"));
        int maxBackgroundCompactions = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.max.background.compactions"));
        int maxSubcompactions = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.max.subcompactions"));
        int maxOpenFiles = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.max.open.files"));
        boolean enableRocksDBStats = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("index.rocksdb.stats.enabled"));

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundFlushes(maxBackgroundFlushed)
                .setMaxBackgroundCompactions(maxBackgroundCompactions)
                .setMaxSubcompactions(maxSubcompactions)
                .setMaxOpenFiles(maxOpenFiles);

        if(enableRocksDBStats)
        {
            int statsInterval = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.stats.interval"));
            String statsPath = ConfigFactory.Instance().getProperty("index.rocksdb.stats.path");
            Statistics stats = new Statistics();
            dbOptions.setStatistics(stats)
                    .setStatsDumpPeriodSec(statsInterval)
                    .setDbLogDir(statsPath);

        }
        RocksDB db = RocksDB.open(dbOptions, rocksDBPath, descriptors, handles);
        if(enableRocksDBStats)
        {
            startRocksDBLogThread(db);
        }
        // 5. Save handles for reuse
        for (int i = 0; i < descriptors.size(); i++)
        {
            String cfName = new String(descriptors.get(i).getName(), StandardCharsets.UTF_8);
            cfHandles.putIfAbsent(cfName, handles.get(i));
        }

        return db;
    }
    private static ColumnFamilyDescriptor createCFDescriptor(byte[] name, Integer keyLen)
    {
        ConfigFactory config = ConfigFactory.Instance();

        long blockSize = Long.parseLong(config.getProperty("index.rocksdb.block.size"));
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setFilterPolicy(new BloomFilter(10, false))
                .setWholeKeyFiltering(false)
                .setBlockSize(blockSize)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
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
        int fixedLengthPrefix = Integer.parseInt(config.getProperty("index.rocksdb.prefix.length"));
        if(keyLen != null)
        {
            fixedLengthPrefix = keyLen + Long.BYTES; // key buffer + index id
        }
        CompactionStyle compactionStyle = CompactionStyle.valueOf(config.getProperty("index.rocksdb.compaction.style"));

        // Compression Options
        CompressionType compressionType = CompressionType.valueOf(config.getProperty("index.rocksdb.compression.type"));
        CompressionType bottommostCompressionType = CompressionType.valueOf(config.getProperty("index.rocksdb.bottommost.compression.type"));

        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
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
                .setCompactionStyle(compactionStyle)
                .useFixedLengthPrefixExtractor(fixedLengthPrefix);

        return new ColumnFamilyDescriptor(name, cfOptions);
    }

    /**
     * Get or create a ColumnFamily for (tableId, indexId).
     */
    public static synchronized ColumnFamilyHandle getOrCreateColumnFamily(long tableId, long indexId, int vNodeId) throws RocksDBException
    {
        String cfName = getCFName(tableId, indexId, vNodeId);

        // Return cached handle if exists
        if (cfHandles.containsKey(cfName))
        {
            return cfHandles.get(cfName);
        }

        Integer keyLen = null;
        try
        {
            keyLen = getIndexKeyLen(tableId, indexId);
        } catch (MetadataException ignored)
        {

        }

        RocksDB db = getRocksDB();
        ColumnFamilyDescriptor newCF = createCFDescriptor(cfName.getBytes(StandardCharsets.UTF_8), keyLen);
        ColumnFamilyHandle handle = db.createColumnFamily(newCF);
        cfHandles.put(cfName, handle);
        return handle;
    }

    protected static synchronized Map<String, ColumnFamilyHandle> getAllCfHandles()
    {
        return cfHandles;
    }

    private static String getCFName(long tableId, long indexId, int vNodeId)
    {
        if(multiCF)
        {
            return "t" + tableId + "_i" + indexId + "_v" + vNodeId;
        }
        else
        {
            return defaultColumnFamily;
        }
    }

    private static long[] parseTableAndIndexId(byte[] cfNameBytes) throws RocksDBException
    {
        if (cfNameBytes == null || Arrays.equals(cfNameBytes, RocksDB.DEFAULT_COLUMN_FAMILY))
        {
            return null;
        }

        String name = new String(cfNameBytes, StandardCharsets.UTF_8);

        try
        {
            // Expected format: "t{tableId}_i{indexId}_v{vNodeId}"
            // Example: "t100_i200_v5" -> ["100", "200", "30"]
            if (name.startsWith("t") && name.contains("_i") && name.contains("_v"))
            {
                // Remove the leading 't'
                String content = name.substring(1);

                // Split using regex for multiple delimiters: _i and _v
                String[] parts = content.split("_i|_v");

                if (parts.length == 3)
                {
                    long tableId = Long.parseLong(parts[0]);
                    long indexId = Long.parseLong(parts[1]);
                    long vNodeId = Long.parseLong(parts[2]);

                    return new long[]{tableId, indexId, vNodeId};
                }
                else
                {
                    throw new RocksDBException("Failed to parse CF name (invalid segments): " + name);
                }
            }
        }
        catch (Exception e)
        {
            throw new RocksDBException("Failed to parse CF name: " + name);
        }

        return null;
    }

    private static Integer getIndexKeyLen(long tableId, long indexId) throws MetadataException
    {
        // Try to retrieve from cache using only indexId
        Integer cachedLen = indexKeyLenCache.get(indexId);
        if (cachedLen != null)
        {
            return cachedLen.equals(VARIABLE_LEN_SENTINEL) ? null : cachedLen;
        }

        // Cache miss: Perform the metadata lookup
        List<Column> keyColumns = IndexUtils.extractInfoFromIndex(tableId, indexId);
        TypeDescription keySchema = TypeDescription.createSchemaFromColumns(keyColumns);

        int keyLen = 0;
        Integer result = null;

        if (keySchema.getChildren() != null)
        {
            boolean isFixedLen = true;
            for (TypeDescription typeDescription : keySchema.getChildren())
            {
                int colLen = keyLengthOf(typeDescription.getCategory().getExternalJavaType());
                if (colLen == -1)
                {
                    isFixedLen = false;
                    break;
                }
                keyLen += colLen;
            }

            if (isFixedLen)
            {
                result = keyLen;
            }
        }

        // Update cache (store result or sentinel)
        indexKeyLenCache.put(indexId, result == null ? VARIABLE_LEN_SENTINEL : result);

        return result;
    }

    public static int keyLengthOf(Class<?> clazz) {
        if (clazz == boolean.class) return 1;
        if (clazz == byte.class)    return 1;
        if (clazz == short.class)   return 2;
        if (clazz == int.class)     return 4;
        if (clazz == long.class)    return 8;
        if (clazz == float.class)   return 4;
        if (clazz == double.class)  return 8;

        // Not Primitive
        return -1;
    }
    public static synchronized RocksDB getRocksDB() throws RocksDBException
    {
        if (instance == null || instance.isClosed())
        {
            instance = createRocksDB(dbPath);
        }
        reference.incrementAndGet();
        return instance;
    }

    public static synchronized void close()
    {
        if (instance != null && reference.decrementAndGet() == 0)
        {
            for (ColumnFamilyHandle handle : cfHandles.values())
            {
                handle.close();
            }
            cfHandles.clear();
            instance.close();
            instance = null;
        }
    }

    static synchronized void clearCFHandles()
    {
        cfHandles.clear();
    }

    public static synchronized String getDbPath()
    {
        return dbPath;
    }

    private static void startRocksDBLogThread(RocksDB db)
    {
        int logInterval = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.log.interval"));
        List<RocksDB> dbList = Collections.singletonList(db);

        scheduler.scheduleAtFixedRate(() ->
        {
            try
            {
                // 1. Get RocksDB Native Metrics
                Map<MemoryUsageType, Long> memoryUsage = MemoryUtil.getApproximateMemoryUsageByType(dbList, null);
                long tableReaders = memoryUsage.getOrDefault(MemoryUsageType.kTableReadersTotal, 0L);
                long memTable = memoryUsage.getOrDefault(MemoryUsageType.kMemTableTotal, 0L);
                long blockCacheOnly = db.getLongProperty("rocksdb.block-cache-usage");
                long indexFilterOnly = Math.max(0, tableReaders - blockCacheOnly);
                long totalNativeBytes = tableReaders + memTable;

                // 2. Get JVM Heap Metrics
                Runtime runtime = Runtime.getRuntime();
                long heapMax = runtime.maxMemory();
                long heapCommitted = runtime.totalMemory();
                long heapUsed = heapCommitted - runtime.freeMemory();

                // 3. Format string with both RocksDB and JVM data
                // We use GiB for all units to keep the Shell script calculations simple
                double GiB = 1024.0 * 1024.0 * 1024.0;

                String formattedMetrics = String.format(
                        "TotalNative≈%.4f GiB (MemTable=%.4f GiB, BlockCache=%.4f GiB, IndexFilter=%.4f GiB) " +
                                "JVMHeap (Used=%.4f GiB, Committed=%.4f GiB, Max=%.4f GiB)",
                        totalNativeBytes / GiB,
                        memTable / GiB,
                        blockCacheOnly / GiB,
                        indexFilterOnly / GiB,
                        heapUsed / GiB,
                        heapCommitted / GiB,
                        heapMax / GiB
                );

                logger.info("[RocksDB Metrics] {}", formattedMetrics);
            }
            catch (Exception e)
            {
                logger.error("Error occurred during RocksDB metrics collection", e);
            }
        }, 0, logInterval, TimeUnit.SECONDS);
    }
}
