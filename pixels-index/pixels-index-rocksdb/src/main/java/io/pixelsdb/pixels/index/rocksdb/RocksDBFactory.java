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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private static RocksDB instance;
    /**
     * The reference counter.
     */
    private static AtomicInteger reference = new AtomicInteger(0);

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
                .map(name -> {
                    ConfigFactory config = ConfigFactory.Instance();
                    long writeBufferSize = Long.parseLong(config.getProperty("index.rocksdb.write.buffer.size"));
                    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
                    tableConfig.setFilterPolicy(new BloomFilter(10, false));
                    tableConfig.setWholeKeyFiltering(false);
                    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
                    cfOptions.setWriteBufferSize(writeBufferSize);
                    cfOptions.setMemtablePrefixBloomSizeRatio(0.1);
                    cfOptions.setTableFormatConfig(tableConfig);
                    return new ColumnFamilyDescriptor(name, cfOptions);
                })
                .collect(Collectors.toList());
        // 4. Open database
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
        return RocksDB.open(dbOptions, dbPath, descriptors, handles);
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
