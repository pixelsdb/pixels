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

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    /**
     * The reference counter.
     */
    private static AtomicInteger reference = new AtomicInteger(0);
    private static final Map<String, ColumnFamilyHandle> cfHandles = new ConcurrentHashMap<>();
    private static final String defaultColumnFamily = new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8);

    private RocksDBFactory() { }

    private static RocksDB createRocksDB() throws RocksDBException
    {
        // 1. Get existing column families (returns empty list for new database)
        List<byte[]> existingColumnFamilies;
        try
        {
            existingColumnFamilies = RocksDB.listColumnFamilies(new Options(), dbPath);
        } catch (RocksDBException e)
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
        // 3. Prepare column family descriptors
        List<ColumnFamilyDescriptor> descriptors = existingColumnFamilies.stream()
                .map(RocksDBFactory::createCFDescriptor)
                .collect(Collectors.toList());

        // 4. Open DB
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        RocksDB db = RocksDB.open(dbOptions, dbPath, descriptors, handles);

        // 5. Save handles for reuse
        for (int i = 0; i < descriptors.size(); i++)
        {
            String cfName = new String(descriptors.get(i).getName(), StandardCharsets.UTF_8);
            cfHandles.putIfAbsent(cfName, handles.get(i));
        }

        return db;
    }
    private static ColumnFamilyDescriptor createCFDescriptor(byte[] name)
    {
        ConfigFactory config = ConfigFactory.Instance();
        long writeBufferSize = Long.parseLong(config.getProperty("index.rocksdb.write.buffer.size"));
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setFilterPolicy(new BloomFilter(10, false))
                .setWholeKeyFiltering(false);
        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .setWriteBufferSize(writeBufferSize)
                .setMemtablePrefixBloomSizeRatio(0.1)
                .setTableFormatConfig(tableConfig);
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

        RocksDB db = getRocksDB();
        ColumnFamilyDescriptor newCF = createCFDescriptor(cfName.getBytes(StandardCharsets.UTF_8));
        ColumnFamilyHandle handle = db.createColumnFamily(newCF);
        cfHandles.put(cfName, handle);
        return handle;
    }

    public static synchronized ColumnFamilyHandle getDefaultColumnFamily()
    {
        return cfHandles.get(defaultColumnFamily);
    }

    private static String getCFName(long tableId, long indexId)
    {
        if(multiCF)
        {
            return "t" + tableId + "_i" + indexId;
        } else
        {
            return defaultColumnFamily;
        }
    }

    public static synchronized RocksDB getRocksDB() throws RocksDBException
    {
        if (instance == null || instance.isClosed())
        {
            instance = createRocksDB();
        }
        reference.incrementAndGet();
        return instance;
    }

    public static synchronized void close()
    {
        if (instance != null && reference.decrementAndGet() == 0)
        {
            instance.close();
            instance = null;
        }
    }

    public static synchronized String getDbPath()
    {
        return dbPath;
    }
}
