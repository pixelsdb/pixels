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
package io.pixelsdb.pixels.index.rocksdb;

import org.junit.Test;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author hank
 * @create 2025-02-09
 */
public class TestRocksDB
{
    static
    {
        RocksDB.loadLibrary();
    }

    @Test
    public void testBasic()
    {
        final String dbPath = "/tmp/rocksdb";

        try (final Options options = new Options();
             final Filter bloomFilter = new BloomFilter(10);
             final ReadOptions readOptions = new ReadOptions().setFillCache(false);
             final Statistics stats = new Statistics();
             final RateLimiter rateLimiter = new RateLimiter(10000000, 10000, 10))
        {
            try (final RocksDB db = RocksDB.open(options, dbPath))
            {
                assert (false);
            }
            catch (final RocksDBException e)
            {
                System.out.format("Caught the expected exception -- %s\n", e);
            }

            try
            {
                options.setCreateIfMissing(true)
                        .setStatistics(stats)
                        .setWriteBufferSize(8 * SizeUnit.KB)
                        .setMaxWriteBufferNumber(3)
                        .setMaxBackgroundJobs(10)
                        .setCompressionType(CompressionType.ZLIB_COMPRESSION)
                        .setCompactionStyle(CompactionStyle.UNIVERSAL);
            }
            catch (final IllegalArgumentException e)
            {
                assert (false);
            }

            assert (options.createIfMissing());
            assert (options.writeBufferSize() == 8 * SizeUnit.KB);
            assert (options.maxWriteBufferNumber() == 3);
            assert (options.maxBackgroundJobs() == 10);
            assert (options.compressionType() == CompressionType.ZLIB_COMPRESSION);
            assert (options.compactionStyle() == CompactionStyle.UNIVERSAL);

            assert (options.memTableFactoryName().equals("SkipListFactory"));
            options.setMemTableConfig(
                    new HashSkipListMemTableConfig()
                            .setHeight(4)
                            .setBranchingFactor(4)
                            .setBucketCount(2000000));
            assert (options.memTableFactoryName().equals("HashSkipListRepFactory"));

            options.setMemTableConfig(
                    new HashLinkedListMemTableConfig()
                            .setBucketCount(100000));
            assert (options.memTableFactoryName().equals("HashLinkedListRepFactory"));

            options.setMemTableConfig(
                    new VectorMemTableConfig().setReservedSize(10000));
            assert (options.memTableFactoryName().equals("VectorRepFactory"));

            options.setMemTableConfig(new SkipListMemTableConfig());
            assert (options.memTableFactoryName().equals("SkipListFactory"));

            options.setTableFormatConfig(new PlainTableConfig());
            // Plain-Table requires mmap read
            options.setAllowMmapReads(true);
            assert (options.tableFactoryName().equals("PlainTable"));

            options.setRateLimiter(rateLimiter);

            final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
            Cache cache = new LRUCache(64 * 1024, 6);
            table_options.setBlockCache(cache)
                    .setFilterPolicy(bloomFilter)
                    .setBlockSizeDeviation(5)
                    .setBlockRestartInterval(10)
                    .setCacheIndexAndFilterBlocks(true);

            assert (table_options.blockSizeDeviation() == 5);
            assert (table_options.blockRestartInterval() == 10);
            assert (table_options.cacheIndexAndFilterBlocks());

            options.setTableFormatConfig(table_options);
            assert (options.tableFactoryName().equals("BlockBasedTable"));

            try (final RocksDB db = RocksDB.open(options, dbPath))
            {
                db.put("hello".getBytes(), "world".getBytes());

                final byte[] value = db.get("hello".getBytes());
                assert ("world".equals(new String(value)));

                final String str = db.getProperty("rocksdb.stats");
                assert (str != null && !str.equals(""));
            }
            catch (final RocksDBException e)
            {
                System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
                assert (false);
            }

            try (final RocksDB db = RocksDB.open(options, dbPath))
            {
                db.put("hello".getBytes(), "world".getBytes());
                byte[] value = db.get("hello".getBytes());
                System.out.format("Get('hello') = %s\n", new String(value));

                for (int i = 1; i <= 9; ++i)
                {
                    for (int j = 1; j <= 9; ++j)
                    {
                        db.put(String.format("%dx%d", i, j).getBytes(), String.format("%d", i * j).getBytes());
                    }
                }

                for (int i = 1; i <= 9; ++i)
                {
                    for (int j = 1; j <= 9; ++j)
                    {
                        System.out.format("%s ", new String(db.get(String.format("%dx%d", i, j).getBytes())));
                    }
                    System.out.println("");
                }

                // write batch test
                try (final WriteOptions writeOpt = new WriteOptions())
                {
                    for (int i = 10; i <= 19; ++i)
                    {
                        try (final WriteBatch batch = new WriteBatch())
                        {
                            for (int j = 10; j <= 19; ++j)
                            {
                                batch.put(String.format("%dx%d", i, j).getBytes(),
                                        String.format("%d", i * j).getBytes());
                            }
                            db.write(writeOpt, batch);
                        }
                    }
                }
                for (int i = 10; i <= 19; ++i)
                {
                    for (int j = 10; j <= 19; ++j)
                    {
                        assert (new String(db.get(String.format("%dx%d", i, j).getBytes())).equals(
                                String.format("%d", i * j)));
                        System.out.format("%s ", new String(db.get(String.format("%dx%d", i, j).getBytes())));
                    }
                    System.out.println("");
                }

                value = db.get("1x1".getBytes());
                assert (value != null);
                value = db.get("world".getBytes());
                assert (value == null);
                value = db.get(readOptions, "world".getBytes());
                assert (value == null);

                final byte[] testKey = "asdf".getBytes();
                final byte[] testValue =
                        "asdfghjkl;'?><MNBVCXZQWERTYUIOP{+_)(*&^%$#@".getBytes();
                db.put(testKey, testValue);
                byte[] testResult = db.get(testKey);
                assert (testResult != null);
                assert (Arrays.equals(testValue, testResult));
                assert (new String(testValue).equals(new String(testResult)));
                testResult = db.get(readOptions, testKey);
                assert (testResult != null);
                assert (Arrays.equals(testValue, testResult));
                assert (new String(testValue).equals(new String(testResult)));

                final byte[] insufficientArray = new byte[10];
                final byte[] enoughArray = new byte[50];
                int len;
                len = db.get(testKey, insufficientArray);
                assert (len > insufficientArray.length);
                len = db.get("asdfjkl;".getBytes(), enoughArray);
                assert (len == RocksDB.NOT_FOUND);
                len = db.get(testKey, enoughArray);
                assert (len == testValue.length);

                len = db.get(readOptions, testKey, insufficientArray);
                assert (len > insufficientArray.length);
                len = db.get(readOptions, "asdfjkl;".getBytes(), enoughArray);
                assert (len == RocksDB.NOT_FOUND);
                len = db.get(readOptions, testKey, enoughArray);
                assert (len == testValue.length);

                db.delete(testKey);
                len = db.get(testKey, enoughArray);
                assert (len == RocksDB.NOT_FOUND);

                // repeat the test with WriteOptions
                try (final WriteOptions writeOpts = new WriteOptions())
                {
                    writeOpts.setSync(true);
                    writeOpts.setDisableWAL(false);
                    db.put(writeOpts, testKey, testValue);
                    len = db.get(testKey, enoughArray);
                    assert (len == testValue.length);
                    assert (new String(testValue).equals(new String(enoughArray, 0, len)));
                }

                try
                {
                    for (final TickerType statsType : TickerType.values())
                    {
                        if (statsType != TickerType.TICKER_ENUM_MAX)
                        {
                            stats.getTickerCount(statsType);
                        }
                    }
                    System.out.println("getTickerCount() passed.");
                }
                catch (final Exception e)
                {
                    System.out.println("Failed in call to getTickerCount()");
                    assert (false); //Should never reach here.
                }

                try
                {
                    for (final HistogramType histogramType : HistogramType.values())
                    {
                        if (histogramType != HistogramType.HISTOGRAM_ENUM_MAX)
                        {
                            HistogramData data = stats.getHistogramData(histogramType);
                        }
                    }
                    System.out.println("getHistogramData() passed.");
                }
                catch (final Exception e)
                {
                    System.out.println("Failed in call to getHistogramData()");
                    assert (false); //Should never reach here.
                }

                try (final RocksIterator iterator = db.newIterator())
                {

                    boolean seekToFirstPassed = false;
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next())
                    {
                        iterator.status();
                        assert (iterator.key() != null);
                        assert (iterator.value() != null);
                        seekToFirstPassed = true;
                    }
                    if (seekToFirstPassed)
                    {
                        System.out.println("iterator seekToFirst tests passed.");
                    }

                    boolean seekToLastPassed = false;
                    for (iterator.seekToLast(); iterator.isValid(); iterator.prev())
                    {
                        iterator.status();
                        assert (iterator.key() != null);
                        assert (iterator.value() != null);
                        seekToLastPassed = true;
                    }

                    if (seekToLastPassed)
                    {
                        System.out.println("iterator seekToLastPassed tests passed.");
                    }

                    iterator.seekToFirst();
                    iterator.seek(iterator.key());
                    assert (iterator.key() != null);
                    assert (iterator.value() != null);

                    System.out.println("iterator seek test passed.");

                }
                System.out.println("iterator tests passed.");

                final List<byte[]> keys = new ArrayList<>();
                try (final RocksIterator iterator = db.newIterator())
                {
                    for (iterator.seekToLast(); iterator.isValid(); iterator.prev())
                    {
                        keys.add(iterator.key());
                    }
                }

                List<byte[]> values = db.multiGetAsList(keys);
                assert (values.size() == keys.size());
                for (final byte[] value1 : values)
                {
                    assert (value1 != null);
                }

                values = db.multiGetAsList(new ReadOptions(), keys);
                assert (values.size() == keys.size());
                for (final byte[] value1 : values)
                {
                    assert (value1 != null);
                }
            }
            catch (final RocksDBException e)
            {
                System.err.println(e);
            }
        }
    }

    @Test
    public void testFullCompaction() throws RocksDBException
    {
        final String dbPath = "/home/ubuntu/disk6/collected_indexes/realtime-pixels-retina-3/rocksdb";
        try (RocksDB rocksDB = RocksDBFactory.createRocksDB(dbPath))
        {
            System.out.println("Start Full Compaction");
            long start = System.currentTimeMillis();

            Map<String, ColumnFamilyHandle> cfHandles = RocksDBFactory.getAllCfHandles();
            // Iterate through each Column Family and trigger a manual compaction
            for (Map.Entry<String, ColumnFamilyHandle> entry : cfHandles.entrySet()) {
                String cfName = entry.getKey();
                ColumnFamilyHandle handle = entry.getValue();
                System.out.println("Compacting Column Family: " + cfName);
                /* * compactRange(handle) triggers a full compaction for the specific CF.
                 * It processes all levels from 0 to the maximum level.
                 * This is a synchronous/blocking call.
                 */
                rocksDB.compactRange(handle);
            }

            long end = System.currentTimeMillis();
            System.out.println("Compaction Duration: " + (end - start) + "ms" + "\tPath:" + dbPath);
        }
        // Note: Ensure RocksDBFactory or a 'finally' block handles the closing of ColumnFamilyHandles
    }
}
