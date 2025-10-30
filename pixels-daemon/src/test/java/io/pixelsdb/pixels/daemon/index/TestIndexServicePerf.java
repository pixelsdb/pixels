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

package io.pixelsdb.pixels.daemon.index;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.index.IndexServiceProvider;
import io.pixelsdb.pixels.common.index.RowIdAllocator;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.index.IndexProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class TestIndexServicePerf
{
    private static final Logger LOGGER = LogManager.getLogger(TestIndexServicePerf.class);
    private static final IndexService indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);
    private static final MetadataService metadataService = MetadataService.Instance();
    private static final String testSchemaName = "testSchema";
    private static final String testTableName = "testTable";
    private static final String filePath = "file:///tmp/testIndexServicePerf";
    private static final List<Long> tableIds = new ArrayList<>();
    private static final List<Long> indexIds = new ArrayList<>();
    private static final List<RowIdAllocator> rowIdAllocators = new ArrayList<>();
    private static Config config;
    private static class Config
    {
        public final int indexNum = 1;
        public final int opsPerTable = 1000000;
        public final boolean destroyBeforeStart = true;
        public final int idRange = 10_000;
        public final int bucketNum = 4;
        public AccessMode accessMode = AccessMode.uniform;
        public double skewAlpha = 1.0;
        public String indexScheme = "rocksdb";

        public enum AccessMode
        {
            uniform,
            skew
        }

    }

    @BeforeAll
    static void setup() throws MetadataException
    {
        tearDown();
        config = new Config();
        List<Schema> schemas = metadataService.getSchemas();
        boolean exists = schemas.stream()
                .anyMatch(schema -> schema.getName().equals(testSchemaName));

        if (!exists)
        {
            boolean success = metadataService.createSchema(testSchemaName);
            if (!success)
            {
                throw new MetadataException("Failed to create schema: " + testSchemaName);
            }
        }

        for (int localTableId = 0; localTableId < config.indexNum; localTableId++)
        {
            String tableName = testTableName + "_" + localTableId;
            String tableStoragePath = filePath + "/" + tableName;

            Column column = new Column();
            column.setName("id");
            column.setType("int");
            metadataService.createTable(testSchemaName, tableName, Storage.Scheme.file,
                    ImmutableList.of(tableStoragePath),
                    ImmutableList.of(column));


            Table table = metadataService.getTable(testSchemaName, tableName);
            long metaTableId = table.getId();
            tableIds.add(metaTableId);
            Layout latestLayout = metadataService.getLatestLayout(testSchemaName, tableName);
            MetadataProto.SinglePointIndex singlePointIndexProto = MetadataProto.SinglePointIndex.newBuilder()
                    .setIndexScheme(config.indexScheme)
                    .setPrimary(true)
                    .setUnique(true)
                    .setTableId(metaTableId)
                    .setSchemaVersionId(latestLayout.getSchemaVersionId())
                    .build();

            SinglePointIndex singlePointIndex = new SinglePointIndex(singlePointIndexProto);
            metadataService.createSinglePointIndex(singlePointIndex);
            SinglePointIndex primaryIndex = metadataService.getPrimaryIndex(metaTableId);
            indexIds.add(primaryIndex.getId());

            rowIdAllocators.add(new RowIdAllocator(metaTableId, 1000, IndexServiceProvider.ServiceMode.local));
        }

    }

    @AfterAll
    static void tearDown()
    {
        try
        {
            metadataService.dropSchema(testSchemaName);
        } catch (MetadataException ignore)
        {}

    }

    @Test
    public void testUniformIndexServicePerf() throws Exception
    {
        fillSequentialData();
        testMultiThreadUpdate();
    }

    @Test
    public void testSkew0_5IndexServicePerf() throws Exception
    {
        config.accessMode = Config.AccessMode.skew;
        config.skewAlpha = 0.5;
        fillSequentialData();
        testMultiThreadUpdate();
    }

    @Test
    public void testSkew1_0IndexServicePerf() throws Exception
    {
        config.accessMode = Config.AccessMode.skew;
        config.skewAlpha = 1.0;
        fillSequentialData();
        testMultiThreadUpdate();
    }

    private void fillSequentialData() throws Exception
    {
        final long timestamp = 0;
        System.out.println("Start sequential data load: " + config.idRange + " entries per thread * " + config.indexNum + " threads");
        List<Thread> threads = new ArrayList<>();
        AtomicLong counter = new AtomicLong(0);

        for (int t = 0; t < config.indexNum; ++t)
        {
            final int threadId = t;
            threads.add(new Thread(() ->
            {
                RowIdAllocator rowIdAllocator = rowIdAllocators.get(threadId);
                Long tableId = tableIds.get(threadId);
                Long indexId = indexIds.get(threadId);
                byte[] k = new byte[IndexPerfUtil.KEY_LENGTH];

                for (int i = 0; i < config.idRange; ++i)
                {
                    long rowId = 0;
                    try
                    {
                        rowId = rowIdAllocator.getRowId();
                    } catch (IndexException e)
                    {
                        throw new RuntimeException(e);
                    }
                    IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(rowId % 10000)
                            .setRgId(0)
                            .setRgRowOffset(i)
                            .build();

                    IndexPerfUtil.encodeKey(i, k);

                    IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                            .setIndexId(indexId)
                            .setTableId(tableId)
                            .setTimestamp(timestamp)
                            .setKey(ByteString.copyFrom(k)).build();

                    IndexProto.PrimaryIndexEntry primaryIndexEntry = IndexProto.PrimaryIndexEntry.newBuilder()
                            .setIndexKey(indexKey)
                            .setRowId(rowId)
                            .setRowLocation(rowLocation)
                            .build();
                    try
                    {
                        indexService.putPrimaryIndexEntry(primaryIndexEntry);
                    } catch (IndexException e)
                    {
                        throw new RuntimeException(e);
                    }
                    counter.incrementAndGet();
                }
            }));
        }

        long start = System.currentTimeMillis();
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();
        long end = System.currentTimeMillis();
        System.out.printf("Total loaded: %d entries, time: %.2fs%n",
                counter.get(), (end - start) / 1000.0);
    }

    private void testMultiThreadUpdate() throws Exception
    {
        System.out.println("\n--- Starting MultiThreadPerf ---");
        System.err.println("Start update test");
        long start = System.nanoTime();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.indexNum; ++i)
        {
            threads.add(new Thread(new Worker(i)));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        long end = System.nanoTime();
        double sec = (end - start) / 1_000_000_000.0;
        long totalOpsCount = config.opsPerTable * config.indexNum;
        System.out.printf("Total ops: %d, time: %.2fs, throughput: %d ops/s%n",
                totalOpsCount, sec, (long) (totalOpsCount / sec));
    }

    public static class IndexPerfUtil
    {
        private static final int KEY_LENGTH = Integer.BYTES;
        private static final int VALUE_LENGTH = Long.BYTES;       // rowId(8)
        private final Random random = new Random();

        private static void putIntLE(byte[] buffer, int value)
        {
            buffer[0] = (byte) value;
            buffer[1] = (byte) (value >>> 8);
            buffer[2] = (byte) (value >>> 16);
            buffer[3] = (byte) (value >>> 24);
        }

        public static void encodeKey(int key, byte[] buffer)
        {
            putIntLE(buffer, key);
        }

        public static double[] buildSegmentedPowerLawCdf(int range, double alpha, int segments)
        {
            double[] cdf = new double[segments];
            double segmentSize = (double) range / segments;
            double sum = 0.0;

            for (int i = 0; i < segments; i++)
            {
                double start = i * segmentSize + 1;
                double end = Math.min(range, (i + 1) * segmentSize);
                double weight = (1.0 / Math.pow(start, alpha) + 1.0 / Math.pow(end, alpha)) / 2;
                sum += weight;
                cdf[i] = sum;
            }

            for (int i = 0; i < segments; i++)
            {
                cdf[i] /= sum;
            }
            cdf[segments - 1] = 1.0;
            return cdf;
        }

        public static int sampleSegmentedPowerLaw(double[] cdf, int range)
        {
            double u = ThreadLocalRandom.current().nextDouble();
            int low = 0, high = cdf.length - 1;

            while (low < high)
            {
                int mid = (low + high) >>> 1;
                if (cdf[mid] >= u)
                    high = mid;
                else
                    low = mid + 1;
            }

            double segmentSize = (double) range / cdf.length;
            int start = (int) (low * segmentSize);
            int end = (int) Math.min(range, (low + 1) * segmentSize);
            return ThreadLocalRandom.current().nextInt(start, end);
        }
    }

    private static class Worker implements Runnable
    {
        private final Long tableId;
        private final Long indexId;

        private final List<BlockingQueue<Integer>> buckets;
        private final List<Thread> consumers;
        private double[] powerLawCdf = null;
        private final RowIdAllocator rowIdAllocator;
        public Worker(int indexNum)
        {
            this.tableId = tableIds.get(indexNum);
            this.indexId = indexIds.get(indexNum);
            this.rowIdAllocator = rowIdAllocators.get(indexNum);

            this.buckets = new ArrayList<>(config.bucketNum);
            for (int i = 0; i < config.bucketNum; ++i)
            {
                buckets.add(new LinkedBlockingQueue<>());
            }

            this.consumers = new ArrayList<>(config.bucketNum);
            for (int i = 0; i < config.bucketNum; ++i)
            {
                final int bucketId = i;
                Thread consumer = new Thread(() -> consumeBucket(bucketId));
                consumer.setDaemon(true);
                consumer.start();
                consumers.add(consumer);
            }

            powerLawCdf = IndexPerfUtil.buildSegmentedPowerLawCdf(config.idRange, config.skewAlpha, 1000);

        }

        @Override
        public void run()
        {
            for (int i = 0; i < config.opsPerTable; ++i)
            {
                int key = generateKey(i);
                int bucketId = key % config.bucketNum;

                try
                {
                    buckets.get(bucketId).put(key);
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while enqueuing key", e);
                }
            }

            for (BlockingQueue<Integer> q : buckets)
            {
                q.add(-1);
            }

            for (Thread t : consumers)
            {
                try
                {
                    t.join();
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void consumeBucket(int bucketId)
        {
            BlockingQueue<Integer> queue = buckets.get(bucketId);
            try
            {
                byte[] putKeyBuffer = new byte[IndexPerfUtil.KEY_LENGTH];
                while (true)
                {
                    int key = queue.take();
                    if (key == -1)
                        break;
                    long tQuery = System.currentTimeMillis();
                    IndexPerfUtil.encodeKey(key, putKeyBuffer);
                    long rowId = rowIdAllocator.getRowId();
                    IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(rowId % 10000)
                            .setRgId(0)
                            .setRgRowOffset((int)rowId).build();

                    IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                            .setKey(ByteString.copyFrom(putKeyBuffer))
                            .setTimestamp(tQuery)
                            .setIndexId(indexId)
                            .setTableId(tableId)
                            .build();

                    IndexProto.PrimaryIndexEntry primaryIndexEntry = IndexProto.PrimaryIndexEntry.newBuilder()
                            .setIndexKey(indexKey)
                            .setRowLocation(rowLocation)
                            .setRowId(rowId)
                            .build();

                    try
                    {
                        indexService.updatePrimaryIndexEntry(primaryIndexEntry);
                    } catch (IndexException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            } catch (IndexException e)
            {
                throw new RuntimeException(e);
            }
        }

        private int generateKey(int i)
        {
            switch (config.accessMode)
            {
                case uniform:
                {
                    return i % config.idRange;
                }
                case skew:
                {
                    return IndexPerfUtil.sampleSegmentedPowerLaw(powerLawCdf, config.idRange);
                }
                default:
                {
                    throw new IllegalArgumentException("Unsupported access mode " + config.accessMode);
                }
            }
        }
    }
}
