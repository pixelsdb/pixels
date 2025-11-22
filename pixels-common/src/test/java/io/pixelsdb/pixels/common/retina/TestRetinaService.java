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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestRetinaService
{
    private static final int NUM_THREADS = 1; // Number of concurrent threads
    private static final double RPC_PER_SECOND = 1000.0; // Desired RPCs per second
    private static final int ROWS_PER_RPC = 200; // Number of rows per RPC
    private static final double UPDATE_RATIO = 1; // Ratio of updates to total operations
    private static final int TEST_DURATION_SECONDS = 180; // Total test duration in seconds
    private static final int INITIAL_KEYS = 20000; // Initial keys to pre-populate for updates
    private static final AtomicLong primaryKeyCounter = new AtomicLong(0); // Counter for generating unique primary keys
    private static final List<ConcurrentLinkedDeque<IndexProto.IndexKey>> threadLocalKeyDeques = new ArrayList<>(NUM_THREADS);
    private static final ThreadLocal<RetinaService.StreamHandler> threadLocalStreamHandler =
            ThreadLocal.withInitial(() -> RetinaService.Instance().startUpdateStream());

    private static String schemaName = "tpch";
    private static String tableName = "nation";
    private static String[] keyColumnNames = {"n_nationkey"};
    private static List<String> colNames = new ArrayList<>();
    private static SinglePointIndex index;

    @BeforeAll
    public static void setUp() throws MetadataException, InterruptedException, JsonProcessingException
    {
        MetadataService metadataService = MetadataService.Instance();
        Table table = metadataService.getTable(schemaName, tableName);
        List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
        KeyColumns keyColumns = new KeyColumns();
        for (Column column : columns)
        {
            colNames.add(column.getName());
            for (String keyColumn : keyColumnNames)
            {
                if (column.getName().equals(keyColumn))
                {
                    keyColumns.addKeyColumnIds((int) column.getId());
                }
            }
        }
        String keyColumn = new ObjectMapper().writeValueAsString(keyColumns);
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

        System.out.println("Pre-populating data for UPDATE operations...");
        List<Long> initialKeys = LongStream.range(0, INITIAL_KEYS)
                .map(i -> primaryKeyCounter.getAndIncrement())
                .boxed()
                .collect(Collectors.toList());

        final CountDownLatch setupLatch = new CountDownLatch(1);
        final List<IndexProto.IndexKey> allIndexKeys = Collections.synchronizedList(new ArrayList<>());
        updateRecords(initialKeys, null, initialIndexKeys -> {
            allIndexKeys.addAll(initialIndexKeys);
            setupLatch.countDown();
        });
        setupLatch.await();

        int keysPerThread = INITIAL_KEYS / NUM_THREADS;
        for (int i = 0; i < NUM_THREADS; i++)
        {
            int startIndex = i * keysPerThread;
            int endIndex = (i == NUM_THREADS - 1) ? INITIAL_KEYS : startIndex + keysPerThread;
            ConcurrentLinkedDeque<IndexProto.IndexKey> threadDeque = new ConcurrentLinkedDeque<>(allIndexKeys.subList(startIndex, endIndex));
            threadLocalKeyDeques.add(threadDeque);
        }
        System.out.println("Pre-population complete.");
    }

    /**
     * Construct insertion data
     * @param i For example, when i = 0, insert: 0 | name_0 | 0 | comment_0
     * @return IndexKey of the inserted record
     */
    public static IndexProto.IndexKey constructInsertData(long i, RetinaProto.InsertData.Builder insertDataBuilder)
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
        for (int j = 0; j < colNames.size(); j++)
        {
            valueMap.put(colNames.get(j), valueBuilder.getValues(j));
        }

        int len = keyColumnNames.length;
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
     */
    public static void updateRecords(List<Long> insertKeys, List<IndexProto.IndexKey> indexKeys,
                              Consumer<List<IndexProto.IndexKey>> onCompleteCallback)
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

        RetinaService.StreamHandler streamHandler = threadLocalStreamHandler.get();
        try
        {
            CompletableFuture<RetinaProto.UpdateRecordResponse> future = streamHandler.updateRecord(schemaName, tableUpdateData);

            future.whenComplete(((response, throwable) ->
            {
                if (throwable == null)
                {
                    onCompleteCallback.accept(result);
                } else
                {
                    System.err.println("Update failed: " + throwable);
                }
            }));
        } catch (RetinaException e)
        {
            System.out.printf("Update failed: %s\n", e.getMessage());
        }
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
            final ConcurrentLinkedDeque<IndexProto.IndexKey> myKeys = threadLocalKeyDeques.get(i);

            executor.submit(() -> {
                try
                {
                    while (System.currentTimeMillis() < testEndTime)
                    {
                        rateLimiter.acquire(); // Wait for token

                        List<Long> keysToInsert = new ArrayList<>();
                        List<IndexProto.IndexKey> keysToDelete = new ArrayList<>();

                        for (int j = 0; j < ROWS_PER_RPC; j++)
                        {
                            // Decide whether to perform update or insert based on ratio
                            if (ThreadLocalRandom.current().nextDouble() <= UPDATE_RATIO && !myKeys.isEmpty())
                            {
                                // Perform update: delete an old key and insert a new key
                                IndexProto.IndexKey keyToDelete = myKeys.poll();
                                if (keyToDelete != null)
                                {
                                    keysToDelete.add(keyToDelete);
                                    keysToInsert.add(primaryKeyCounter.getAndIncrement());
                                } else
                                {
                                    // If no key is available, perform pure insert
                                    keysToInsert.add(primaryKeyCounter.getAndIncrement());
                                }
                            } else
                            {
                                // Perform pure insert
                                keysToInsert.add(primaryKeyCounter.getAndIncrement());
                            }
                        }

                        if (!keysToInsert.isEmpty() || !keysToDelete.isEmpty())
                        {
                            updateRecords(keysToInsert, keysToDelete, newKeys -> {
                                myKeys.addAll(newKeys); // Add new keys back to the deque
                                totalOperations.incrementAndGet();
                                insertCount.addAndGet(keysToInsert.size());
                                deleteCount.addAndGet(keysToDelete.size());
                            });
                        }
                    }
                } finally
                {
                    latch.countDown();
                }
            });
        }

        latch.await(); // Wait for all threads to finish
        long endTime = System.currentTimeMillis();
        long durationMillis = endTime - startTime;
        executor.shutdown();

        System.out.println("======================================================");
        System.out.println("Test Finished.");
        System.out.printf(" - Total execution time: %.3f seconds\n", durationMillis / 1000.0);
        System.out.printf(" - Total RPC calls: %d\n", totalOperations.get());
        System.out.printf(" - Actual TPS (RPCs/sec): %.2f\n", totalOperations.get() / (durationMillis / 1000.0));
        System.out.println("------------------------------------------------------");
        System.out.printf(" - Total rows inserted: %d\n", insertCount.get());
        System.out.printf(" - Total rows deleted: %d\n", deleteCount.get());
        System.out.println("======================================================");
    }
}
