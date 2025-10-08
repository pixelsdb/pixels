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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestRetinaService
{
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
    public void testStreamUpdateRecord()
    {
        // Insert 10 rows of data.
        List<Long> insertData = LongStream.range(0, 10).boxed().collect(Collectors.toList());
        List<IndexProto.IndexKey> indexKeys = updateRecords(insertData, null);
        System.out.println("You can use trino-cli to query newly inserted data.");

        // Delete these inserted data after 10 seconds.
        try
        {
            System.out.println("The inserted data will be deleted after 10 seconds.");
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        updateRecords(null, indexKeys);
    }

    @Test
    public void testUpdateSingleRecord()
    {
        // Insert a row of data.
        List<Long> initData = new ArrayList<>();
        initData.add(0L);
        List<IndexProto.IndexKey>indexKeys =  updateRecords(initData, null);

        for (int i = 1; i < 1000; ++i)
        {
            List<Long> insertData = new ArrayList<>();
            insertData.add((long) i);
            System.out.println("update from" + (i - 1) + " to " + i);
            indexKeys = updateRecords(insertData, indexKeys);
        }

        // Delete the last inserted data.
        try
        {
            System.out.println("The last inserted data will be deleted after 10 seconds.");
            Thread.sleep(10000);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        updateRecords(null, indexKeys);
    }

    @Test
    public void testInsertAndDeleteMultipleRecords()
    {
        long testSize = 10000;

        List<Long> insertedKeys = new ArrayList<>();
        List<IndexProto.IndexKey> indexKeys = new ArrayList<>();

        // Insert initial data 0-99
        for (long i = 0; i < testSize * 2; ++i)
        {
            RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
            IndexProto.IndexKey key = constructInsertData(i, insertDataBuilder);
            insertedKeys.add(i);
            indexKeys.add(key);
        }
        updateRecords(insertedKeys, null);

        // insert one record and delete three record each time
        long startTime = System.currentTimeMillis();
        for (long i = testSize * 2; i < testSize * 3; ++i)
        {
            List<Long> insertList = new ArrayList<>();
            insertList.add(i);

            List<IndexProto.IndexKey> deleteList = new ArrayList<>();
            if (indexKeys.size() >= 3)
            {
                deleteList.add(indexKeys.get(0));
                deleteList.add(indexKeys.get(1));
                deleteList.add(indexKeys.get(2));
            }

            List<IndexProto.IndexKey> newKeys = updateRecords(insertList, deleteList);

            indexKeys.addAll(newKeys);
            if (indexKeys.size() >= 3)
            {
                indexKeys.subList(0, 3).clear();
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("TestInsertAndDeleteMultipleRecords time cost: " + (endTime - startTime) + " ms");
        System.out.println("TestInsertAndDeleteMultipleRecords TPS: " + testSize * 1000.0 / (endTime - startTime) + " ops/s");
    }
}
