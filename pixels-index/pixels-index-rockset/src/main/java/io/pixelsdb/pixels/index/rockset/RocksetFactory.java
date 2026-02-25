/*
 * Copyright 2025 PixelsDB.
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.index.rockset;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.index.rockset.jni.*;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import java.util.concurrent.atomic.AtomicInteger;
import io.pixelsdb.pixels.core.TypeDescription;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class RocksetFactory
{
    private static final Logger logger = LoggerFactory.getLogger(RocksetFactory.class);
    private static final String dbPath = ConfigFactory.Instance().getProperty("index.rockset.data.path");
    private static final boolean multiCF = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("index.rockset.multicf"));
    private static RocksetDB rocksetDB;
    private static RocksetCache blockCache;
    private static final long blockCacheCapacity = Long.parseLong(ConfigFactory.Instance().getProperty("index.rockset.block.cache.capacity"));
    private static final int blockCacheShardBits = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rockset.block.cache.shard.bits"));
    private static final String bucketName = ConfigFactory.Instance().getProperty("index.rockset.s3.bucket");
    private static final String s3Prefix = ConfigFactory.Instance().getProperty("index.rockset.s3.prefix");
    private static final String localDbPath = ConfigFactory.Instance().getProperty("index.rockset.local.data.path");
    private static final String persistentCachePath = ConfigFactory.Instance().getProperty("index.rockset.persistent.cache.path");
    private static final long persistentCacheSizeGB = Long.parseLong(ConfigFactory.Instance().getProperty("index.rockset.persistent.cache.size.gb"));
    private static final boolean readOnly = Boolean.parseBoolean(ConfigFactory.Instance().getProperty("index.rockset.read.only"));
    /**
     * The reference counter.
     */
    private static final AtomicInteger reference = new AtomicInteger(0);
    private static final Map<String, RocksetColumnFamilyHandle> cfHandles = new ConcurrentHashMap<>();
    private static final String defaultColumnFamily = "default"; // Change for Rockset if needed
    private static final Map<Long, Integer> indexKeyLenCache = new ConcurrentHashMap<>();
    private static final Integer VARIABLE_LEN_SENTINEL = -2;
    private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable ->
    {
        Thread thread = new Thread(runnable, "Rockset-Metrics-Logger");
        thread.setDaemon(true);
        return thread;
    });

    private RocksetFactory() { }

    private static RocksetEnv createRocksetEnv()
    {
        CloudDBOptions dbOptions = new CloudDBOptions().setBucketName(bucketName).setS3Prefix(s3Prefix)
                .setLocalDbPath(localDbPath).setPersistentCachePath(persistentCachePath)
                .setPersistentCacheSizeGB(persistentCacheSizeGB).setReadOnly(readOnly);
        return RocksetEnv.create(dbOptions.getBucketName(),dbOptions.getS3Prefix());
    }
    static RocksetDB createRocksetDB(String rocksetPath) throws Exception
    {
        // 1. Get existing column families (returns empty list for new database)
        List<byte[]> existingColumnFamilies;
        try
        {
            existingColumnFamilies = RocksetDB.listColumnFamilies0(rocksetPath);
        } catch (Exception e)
        {
            // For new database, return list containing only default column family
            existingColumnFamilies = Collections.singletonList(RocksetDB.DEFAULT_COLUMN_FAMILY);
        }
        // 2. Ensure default column family is included
        boolean existDefaultCF = false;
        for (byte[] cf : existingColumnFamilies)
        {
            if (Arrays.equals(cf, RocksetDB.DEFAULT_COLUMN_FAMILY))
            {
                existDefaultCF = true;
                break;
            }
        }
        if (!existDefaultCF)
        {
            existingColumnFamilies = new ArrayList<>(existingColumnFamilies);
            existingColumnFamilies.add(RocksetDB.DEFAULT_COLUMN_FAMILY);
        }

        if (blockCache == null)
        {
            blockCache = new RocksetLRUCache(blockCacheCapacity, blockCacheShardBits);
        }

        // 3. Prepare column family descriptors
        List<RocksetColumnFamilyDescriptor> descriptors = new ArrayList<>();
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
        RocksetEnv rocksetEnv = createRocksetEnv();
        RocksetDB db = RocksetDB.open(rocksetEnv, options, rocksetPath, descriptors, handles);
        if(enableStats)
        {
            startRocksetLogThread(db);
        }

        // 5. Save handles for reuse
        for (int i = 0; i < descriptors.size(); i++)
        {
            String cfName = new String(descriptors.get(i).getName(), StandardCharsets.UTF_8);
            cfHandles.putIfAbsent(cfName, handles.get(i));
        }
        return db;
    }

    private static RocksetColumnFamilyDescriptor createCFDescriptor(byte[] name, Integer keyLen)
    {
        ConfigFactory config = ConfigFactory.Instance();

        long blockSize = Long.parseLong(config.getProperty("index.rockset.block.size"));
        RocksetBlockBasedTableConfig tableConfig = new RocksetBlockBasedTableConfig()
                .setFilterPolicy(new RocksetBloomFilter(10, false))
                .setWholeKeyFiltering(false)
                .setBlockSize(blockSize)
                .setBlockCache(blockCache);

        // ColumnFamily Options
        long writeBufferSize = Long.parseLong(config.getProperty("index.rockset.write.buffer.size"));
        int maxWriteBufferNumber = Integer.parseInt(config.getProperty("index.rockset.max.write.buffer.number"));
        int minWriteBufferNumberToMerge = Integer.parseInt(config.getProperty("index.rockset.min.write.buffer.number.to.merge"));

        // Compaction Options
        int level0FileNumCompactionTrigger = Integer.parseInt(config.getProperty("index.rockset.level0.file.num.compaction.trigger"));
        long maxBytesForLevelBase = Long.parseLong(config.getProperty("index.rockset.max.bytes.for.level.base"));
        int maxBytesForLevelMultiplier = Integer.parseInt(config.getProperty("index.rockset.max.bytes.for.level.multiplier"));
        long targetFileSizeBase = Long.parseLong(config.getProperty("index.rockset.target.file.size.base"));
        int targetFileSizeMultiplier = Integer.parseInt(config.getProperty("index.rockset.target.file.size.multiplier"));
        RocksetCompactionStyle compactionStyle = RocksetCompactionStyle.valueOf(config.getProperty("index.rockset.compaction.style"));
        int fixedLengthPrefix = Integer.parseInt(config.getProperty("index.rockset.prefix.length"));
        if(keyLen != null)
        {
            fixedLengthPrefix = keyLen + Long.BYTES; // key buffer + index id
        }

        // Compression Options
        RocksetCompressionType compressionType = RocksetCompressionType.valueOf(config.getProperty("index.rockset.compression.type"));
        RocksetCompressionType bottommostCompressionType = RocksetCompressionType.valueOf(config.getProperty("index.rockset.bottommost.compression.type"));

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

    public static synchronized RocksetColumnFamilyHandle getOrCreateColumnFamily(long tableId, long indexId, int vNodeId) throws Exception {
        String cfName = getCFName(tableId, indexId, vNodeId);

        // Return cached handle if exists
        if (cfHandles.containsKey(cfName)) {
            return cfHandles.get(cfName);
        }
        Integer keyLen = null;
        try
        {
            keyLen = getIndexKeyLen(tableId, indexId);
        } catch (MetadataException ignored)
        {

        }
        RocksetDB db = getRocksetDB();
        RocksetColumnFamilyDescriptor newCF = createCFDescriptor(cfName.getBytes(StandardCharsets.UTF_8), keyLen);
        RocksetColumnFamilyHandle handle = db.createColumnFamily(newCF);
        cfHandles.put(cfName, handle);
        return handle;
    }

    protected static synchronized Map<String, RocksetColumnFamilyHandle> getAllCfHandles()
    {
        return cfHandles;
    }

    private static String getCFName(long tableId, long indexId, int vNodeId) {
        if(multiCF)
        {
            return "t" + tableId + "_i" + indexId + "_v" + vNodeId;
        }
        else
        {
            return defaultColumnFamily;
        }
    }

    private static long[] parseTableAndIndexId(byte[] cfNameBytes) throws Exception
    {
        if (cfNameBytes == null || Arrays.equals(cfNameBytes, RocksetDB.DEFAULT_COLUMN_FAMILY))
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
                    throw new Exception("Failed to parse CF name (invalid segments): " + name);
                }
            }
        }
        catch (Exception e)
        {
            throw new Exception("Failed to parse CF name: " + name, e);
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

    public static synchronized RocksetDB getRocksetDB() throws Exception {
        if (rocksetDB == null || rocksetDB.isClosed()) {
            rocksetDB = createRocksetDB(dbPath);
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
            rocksetDB.close();
            rocksetDB = null;
        }
    }

    static synchronized void clearCFHandles()
    {
        cfHandles.clear();
    }

    public static synchronized String getDbPath() {
        return dbPath;
    }

    private static void startRocksetLogThread(RocksetDB db)
    {
        int logInterval = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rockset.log.interval"));
        List<RocksetDB> dbList = Collections.singletonList(db);

        scheduler.scheduleAtFixedRate(() ->
        {
            try
            {
                // 1. Get Rockset Native Metrics
                Map<MemoryUsageType, Long> memoryUsage = MemoryUtil.getApproximateMemoryUsageByType(dbList, null);
                long tableReaders = memoryUsage.getOrDefault(MemoryUsageType.kTableReadersTotal, 0L);
                long memTable = memoryUsage.getOrDefault(MemoryUsageType.kMemTableTotal, 0L);
                long blockCacheOnly = db.getLongProperty("rockset.block-cache-usage");
                long indexFilterOnly = Math.max(0, tableReaders - blockCacheOnly);
                long totalNativeBytes = tableReaders + memTable;

                // 2. Get JVM Heap Metrics
                Runtime runtime = Runtime.getRuntime();
                long heapMax = runtime.maxMemory();
                long heapCommitted = runtime.totalMemory();
                long heapUsed = heapCommitted - runtime.freeMemory();

                // 3. Format string with both Rockset and JVM data
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

                logger.info("[Rockset Metrics] {}", formattedMetrics);
            }
            catch (Exception e)
            {
                logger.error("Error occurred during Rockset metrics collection", e);
            }
        }, 0, logInterval, TimeUnit.SECONDS);
    }
}

