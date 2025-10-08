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
package io.pixelsdb.pixels.common.retina;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestRetinaService
{
    private static final int NUM_THREADS = 10; // Number of concurrent threads
    private static final double RPC_PER_SECOND = 1000.0; // Desired RPCs per second
    private static final int ROWS_PER_RPC = 10; // Number of rows per RPC
    private static final double UPDATE_RATIO = 0.8; // Ratio of updates to total operations
    private static final int TEST_DURATION_SECONDS = 60; // Total test duration in seconds
    private static final AtomicLong primaryKeyCounter = new AtomicLong(0); // Counter for generating unique primary keys
    private static final ConcurrentLinkedDeque<IndexProto.IndexKey> existingKeys = new ConcurrentLinkedDeque<>(); // Thread-safe deque to store existing keys

    private static String schemaName;
    private static String tableName;
    private static String[] colNames;
    private static SinglePointIndex index;

    @BeforeAll
    public static void setUp() throws MetadataException
    {
        schemaName = "tpch";
        tableName = "nation";
        colNames = new String[]{"key", "name", "region", "comment"};
        MetadataService metadataService = MetadataService.Instance();

        String keyColumn = "{\"keyColumnIds\":[25]}";   // Retrieve the primary key column ID from `COLS` table.
        Table table = metadataService.getTable(schemaName, tableName);
        Layout layout = metadataService.getLatestLayout(schemaName, tableName);
        MetadataProto.SinglePointIndex.Builder singlePointIndexBuilder = MetadataProto.SinglePointIndex.newBuilder()
                .setId(0L)
                .setKeyColumns(keyColumn)
                .setPrimary(true)
                .setUnique(true)
                .setIndexScheme("rocksdb")
                .setTableId(table.getId())
                .setSchemaVersionId(layout.getSchemaVersionId());

        SinglePointIndex singlePointIndex = new SinglePointIndex(singlePointIndexBuilder.build());
        boolean result = metadataService.createSinglePointIndex(singlePointIndex);
        Assertions.assertTrue(result);
        index = metadataService.getPrimaryIndex(table.getId());

        //
        System.out.println("Pre-populating data for UPDATE operations...");
        List<Long> initialKeys = LongStream.range(0, NUM_THREADS * ROWS_PER_RPC * 10)
                .map(i -> primaryKeyCounter.getAndIncrement())
                .boxed()
                .collect(Collectors.toList());
        List<IndexProto.IndexKey> initialIndexKeys = new TestRetinaService().updateRecords(initialKeys, null);
        existingKeys.addAll(initialIndexKeys);
        System.out.println("Pre-population complete.");
    }

    /**
     * Construct insertion data
     * @param i For example, when i = 0, insert: 0 | name_0 | 0 | comment_0
     * @return IndexKey of the inserted record
     */
    public IndexProto.IndexKey constructInsertData(long i, RetinaProto.InsertData.Builder insertDataBuilder)
    {
        byte[][] cols = new byte[4][];
        cols[0] = ByteBuffer.allocate(8).putLong(i).array();
        cols[1] = ("name_" + i).getBytes();
        cols[2] = ByteBuffer.allocate(8).putLong(i).array();
        cols[3] = ("comment_" + i).getBytes();

        SinkProto.RowValue.Builder valueBuilder = SinkProto.RowValue.newBuilder();
        for (int j = 0; j < 4; ++j)
        {
            SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder()
                    .setValue(ByteString.copyFrom(cols[j]));
            valueBuilder.addValues(columnValueBuilder.build());
        }
        Map<String, SinkProto.ColumnValue> valueMap = new HashMap<>();
        for (int j = 0; j < colNames.length; j++)
        {
            valueMap.put(colNames[j], valueBuilder.getValues(j));
        }

        List<String> keyColumnNames = new LinkedList<>();
        keyColumnNames.add("key"); // 'key' is the primary key's name
        int len = keyColumnNames.size();
        List<ByteString> keyColumnValues = new ArrayList<>(len);
        int keySize = 0;
        for (String keyColumnName : keyColumnNames)
        {
            ByteString value = valueMap.get(keyColumnName).getValue();
            keyColumnValues.add(value);
            keySize += value.size();
        }
        keySize += Long.BYTES + (len + 1) * 2 + Long.BYTES;
        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);
        byteBuffer.putLong(index.getTableId()).putChar(':');
        for (ByteString value : keyColumnValues)
        {
            byteBuffer.put(value.toByteArray());
            byteBuffer.putChar(':');
        }
        byteBuffer.putLong(0); // timestamp
        byteBuffer.flip();
        IndexProto.IndexKey indexKey = IndexProto.IndexKey.newBuilder()
                .setTimestamp(0)
                .setKey(ByteString.copyFrom(byteBuffer))
                .setIndexId(index.getId())
                .setTableId(index.getTableId())
                .build();

        insertDataBuilder.addIndexKeys(indexKey)
                .addColValues(ByteString.copyFrom(cols[0]))
                .addColValues(ByteString.copyFrom(cols[1]))
                .addColValues(ByteString.copyFrom(cols[2]))
                .addColValues(ByteString.copyFrom(cols[3]));

        return indexKey;
    }

    /**
     * Update records
     * @param insertKeys : parameter for constructing the insert data
     * @param indexKeys : parameter for constructing the delete data
     * @return IndexKey of the inserted record
     */
    public List<IndexProto.IndexKey> updateRecords(List<Long> insertKeys, List<IndexProto.IndexKey> indexKeys)
    {
        List<IndexProto.IndexKey> result = new ArrayList<>();

        List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
        RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder = RetinaProto.TableUpdateData.newBuilder()
                .setTableName(tableName).setPrimaryIndexId(index.getId()).setTimestamp(0L);

        if (insertKeys != null)
        {
            for (Long insertKey : insertKeys)
            {
                RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
                IndexProto.IndexKey indexKey = constructInsertData(insertKey, insertDataBuilder);
                result.add(indexKey);
                tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
            }
        }

        if (indexKeys != null)
        {
            for (IndexProto.IndexKey indexKey : indexKeys)
            {
                RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder();
                deleteDataBuilder.addIndexKeys(indexKey);
                tableUpdateDataBuilder.addDeleteData(deleteDataBuilder.build());
            }
        }

        tableUpdateData.add(tableUpdateDataBuilder.build());

        try (RetinaService.StreamHandler streamHandler = RetinaService.Instance().startUpdateStream())
        {
            streamHandler.updateRecord(schemaName, tableUpdateData);
        }
        return result;
    }

    @Test
    public void configurableLoadTest() throws InterruptedException
    {
        System.out.println("======================================================");
        System.out.printf("Starting Load Test with configuration:\n");
        System.out.printf(" - Threads: %d\n", NUM_THREADS);
        System.out.printf(" - Target RPCs/sec: %.2f\n", RPC_PER_SECOND);
        System.out.printf(" - Rows per RPC: %d\n", ROWS_PER_RPC);
        System.out.printf(" - Update Ratio: %.2f\n", UPDATE_RATIO);
        System.out.printf(" - Duration: %d seconds\n", TEST_DURATION_SECONDS);
        System.out.println("======================================================");

        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        final RateLimiter rateLimiter = RateLimiter.create(RPC_PER_SECOND);
        final AtomicLong totalOperations = new AtomicLong(0);
        final AtomicLong insertCount = new AtomicLong(0);
        final AtomicLong deleteCount = new AtomicLong(0);
        final CountDownLatch latch = new CountDownLatch(NUM_THREADS);
        final long testEndTime = System.currentTimeMillis() + TEST_DURATION_SECONDS * 1000L;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < NUM_THREADS; i++)
        {
            executor.submit(() -> {
                try {
                    while (System.currentTimeMillis() < testEndTime) {
                        rateLimiter.acquire(); // 等待令牌

                        List<Long> keysToInsert = new ArrayList<>();
                        List<IndexProto.IndexKey> keysToDelete = new ArrayList<>();

                        for (int j = 0; j < ROWS_PER_RPC; j++) {
                            // 根据比例决定是执行更新还是插入
                            if (ThreadLocalRandom.current().nextDouble() < UPDATE_RATIO && !existingKeys.isEmpty()) {
                                // 执行更新操作: 从队列中取一个旧key来删除，并生成一个新key来插入
                                IndexProto.IndexKey keyToDelete = existingKeys.poll();
                                if (keyToDelete != null) {
                                    keysToDelete.add(keyToDelete);
                                    keysToInsert.add(primaryKeyCounter.getAndIncrement());
                                } else {
                                    // 如果没取到（例如队列暂时为空），则本次操作转为纯插入
                                    keysToInsert.add(primaryKeyCounter.getAndIncrement());
                                }
                            } else {
                                // 执行纯插入操作
                                keysToInsert.add(primaryKeyCounter.getAndIncrement());
                            }
                        }

                        if (!keysToInsert.isEmpty() || !keysToDelete.isEmpty()) {
                            List<IndexProto.IndexKey> newKeys = updateRecords(keysToInsert, keysToDelete);
                            existingKeys.addAll(newKeys); // 将新生成的key放回队列
                            totalOperations.incrementAndGet();
                            insertCount.addAndGet(keysToInsert.size());
                            deleteCount.addAndGet(keysToDelete.size());
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(); // 等待所有线程完成
        executor.shutdown();
        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;

        System.out.println("======================================================");
        System.out.println("Test Finished.");
        System.out.printf(" - Total execution time: %.3f seconds\n", durationMillis / 1000.0);
        System.out.printf(" - Total RPC calls: %d\n", totalOperations.get());
        System.out.printf(" - Actual TPS (RPCs/sec): %.2f\n", totalOperations.get() / (durationMillis / 1000.0));
        System.out.println("------------------------------------------------------");
        System.out.printf(" - Total rows inserted: %d\n", insertCount.get());
        System.out.printf(" - Total rows deleted: %d\n", deleteCount.get());
        System.out.printf(" - An UPDATE operation consists of 1 DELETE and 1 INSERT.\n");
        System.out.println("======================================================");
    }
}
