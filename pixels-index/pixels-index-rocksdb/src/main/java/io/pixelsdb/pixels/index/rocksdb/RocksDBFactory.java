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
import io.pixelsdb.pixels.common.metadata.MetadataCache;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.IndexUtils;
import io.pixelsdb.pixels.core.TypeDescription;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @package: io.pixelsdb.pixels.index.rocksdb
 * @className: RocksDBFactory
 * @author: AntiO2
 * @date: 2025/9/8 10:33
 */
public class RocksDBFactory
{
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
            String statsPath = ConfigFactory.Instance().getProperty("index.rocksdb.stats.path");
            int statsInterval = Integer.parseInt(ConfigFactory.Instance().getProperty("index.rocksdb.stats.interval"));
            Statistics stats = new Statistics();
            dbOptions.setStatistics(stats)
                    .setStatsDumpPeriodSec(statsInterval)
                    .setDbLogDir(statsPath);
        }

        RocksDB db = RocksDB.open(dbOptions, rocksDBPath, descriptors, handles);

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
    public static synchronized ColumnFamilyHandle getOrCreateColumnFamily(long tableId, long indexId) throws RocksDBException
    {
        String cfName = getCFName(tableId, indexId);

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

    private static String getCFName(long tableId, long indexId)
    {
        if(multiCF)
        {
            return "t" + tableId + "_i" + indexId;
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
            // Remove the leading 't', then split by '_i'
            // Example: "t123_i456" -> ["123", "456"]
            if (name.startsWith("t") && name.contains("_i"))
            {
                String parts = name.substring(1); // Remove 't'
                String[] ids = parts.split("_i");

                if (ids.length == 2)
                {
                    long tableId = Long.parseLong(ids[0]);
                    long indexId = Long.parseLong(ids[1]);
                    return new long[]{tableId, indexId};
                } else
                {
                    throw new RocksDBException("Failed to parse CF name: " + name);
                }
            }
        } catch (Exception e)
        {
            throw new RocksDBException("Failed to parse CF name: " + name);
        }
        return null;
    }

    private static Integer getIndexKeyLen(long tableId, long indexId) throws MetadataException
    {
        List<Column> keyColumns = IndexUtils.extractInfoFromIndex(tableId, indexId);
        TypeDescription keySchema = TypeDescription.createSchemaFromColumns(keyColumns);
        int keyLen = 0;
        if (keySchema.getChildren() != null)
        {
            for(TypeDescription typeDescription: keySchema.getChildren())
            {
                int colLen = keyLengthOf(typeDescription.getCategory().getExternalJavaType());
                if(colLen == -1)
                {
                    return null;
                }
                keyLen += colLen;
            }
            return keyLen;
        } else
        {
            return null;
        }
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

    public static synchronized String getDbPath()
    {
        return dbPath;
    }
}
