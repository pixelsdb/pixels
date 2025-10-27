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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.IndexService;
import io.pixelsdb.pixels.common.index.IndexServiceProvider;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestIndexServicePerf
{
    private static final IndexService indexService = IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.local);
    private static final MetadataService metadataService = MetadataService.Instance();
    private static final String testSchemaName = "testSchema";
    private static final String testTableName = "testTable";
    private static final String filePath = "file:///tmp/testIndexServicePerf";
    private static final List<Long> tableIds = new ArrayList<>();
    private static final List<Long> indexIds = new ArrayList<>();
    private static Config config;

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

        for (int localTableId = 0; localTableId < config.threadNum; localTableId++)
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
                    .setIndexScheme("rocksdb")
                    .setPrimary(true)
                    .setUnique(true)
                    .setTableId(metaTableId)
                    .setSchemaVersionId(latestLayout.getSchemaVersionId())
                    .build();

            SinglePointIndex singlePointIndex = new SinglePointIndex(singlePointIndexProto);
            metadataService.createSinglePointIndex(singlePointIndex);
            SinglePointIndex primaryIndex = metadataService.getPrimaryIndex(metaTableId);
            indexIds.add(primaryIndex.getId());
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
    public void setDb() throws MetadataException
    {

    }

    @Test
    public void testIndexServicePerf() throws Exception
    {
        fillSequentialData();
        testMultiThreadUpdate();
    }

    private void fillSequentialData() throws Exception
    {
        final long timestamp = 0;
        System.out.println("Start sequential data load: " + config.idRange + " entries per thread * " + config.threadNum + " threads");
        List<Thread> threads = new ArrayList<>();
        AtomicLong counter = new AtomicLong(0);

        for (int t = 0; t < config.threadNum; ++t)
        {
            final int threadId = t;
            threads.add(new Thread(() ->
            {
                Long tableId = tableIds.get(threadId);
                Long indexId = indexIds.get(threadId);
                byte[] k = new byte[RocksDBUtil.KEY_LENGTH];

                for (int rowId = 0; rowId < config.idRange; ++rowId)
                {

                    IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(0)
                            .setRgId(0)
                            .setRgRowOffset(rowId).build();

                    RocksDBUtil.encodeKey(rowId, k);

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
        for (int i = 0; i < config.threadNum; ++i)
        {
            threads.add(new Thread(new Worker(tableIds.get(i), indexIds.get(i))));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        long end = System.nanoTime();
        double sec = (end - start) / 1_000_000_000.0;
        long totalOpsCount = config.opsPerThread * config.threadNum;
        System.out.printf("Total ops: %d, time: %.2fs, throughput: %d ops/s%n",
                totalOpsCount, sec, (long) (totalOpsCount / sec));
    }

    public static class Config
    {
        public final int threadNum = 16;
        public final int opsPerThread = 1000000;
        public final boolean destroyBeforeStart = true;
        public final int idRange = 10_000_000;
    }

    public static class RocksDBUtil
    {
        private static final int KEY_LENGTH = Integer.BYTES;
        private static final int VALUE_LENGTH = Long.BYTES;       // rowId(8)

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


        public static long decodeLong(byte[] bytes)
        {
            if (bytes == null || bytes.length < VALUE_LENGTH)
            {
                return -1;
            }
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }
    }

    private static class Worker implements Runnable
    {

        private final Long tableId;
        private final Long indexId;
        private final byte[] putKeyBuffer;
        private long nextRowId;

        public Worker(long tableId, long indexId)
        {
            this.tableId = tableId;
            this.indexId = indexId;
            this.nextRowId = config.idRange;
            this.putKeyBuffer = new byte[RocksDBUtil.KEY_LENGTH];
        }

        @Override
        public void run()
        {

            {
                for (int i = 0; i < config.opsPerThread; ++i)
                {
                    int key = i % config.idRange;
                    long tQuery = System.currentTimeMillis();


                    RocksDBUtil.encodeKey(key, putKeyBuffer);

                    IndexProto.RowLocation rowLocation = IndexProto.RowLocation.newBuilder()
                            .setFileId(0)
                            .setRgId(0)
                            .setRgRowOffset((int) nextRowId).build();

                    IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                            .setKey(ByteString.copyFrom(putKeyBuffer))
                            .setTimestamp(tQuery)
                            .setIndexId(indexId)
                            .setTableId(tableId)
                            .build();

                    IndexProto.PrimaryIndexEntry primaryIndexEntry = IndexProto.PrimaryIndexEntry.newBuilder()
                            .setIndexKey(indexKey)
                            .setRowLocation(rowLocation)
                            .setRowId(nextRowId++)
                            .build();
                    try
                    {
                        indexService.updatePrimaryIndexEntry(primaryIndexEntry);
                    } catch (IndexException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
