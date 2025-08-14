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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestRetinaService
{
    private static String schemaName;
    private static String tableName;
    private static SinglePointIndex index;

    @BeforeAll
    public static void setUp() throws MetadataException
    {
        schemaName = "tpch";
        tableName = "nation";
        MetadataService metadataService = MetadataService.Instance();

        String keyColumn = "{\"keyColumnIds\":[25]}";
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

    @Test
    public void testStreamUpdateRecord()
    {
        String[] colNames = {"key", "name", "region", "comment"};
        try (RetinaService.StreamHandle streamHandle = RetinaService.Instance().startUpdateStream();)
        {
            for (int i = 0; i < 10; ++i)
            {
                List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
                RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder = RetinaProto.TableUpdateData.newBuilder()
                        .setTableName(tableName).setPrimaryIndexId(index.getId());
                byte[][] cols = new byte[4][];
                cols[0] = Integer.toString(i).getBytes();
                cols[1] = ("name_" + i).getBytes();
                cols[2] = Integer.toString(i).getBytes();
                cols[3] = ("comment_" + i).getBytes();

                SinkProto.RowValue.Builder valueBuilder = SinkProto.RowValue.newBuilder();
                for (int j = 0; j < 4; ++j)
                {
                    SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder()
                            .setName(colNames[j])
                            .setValue(ByteString.copyFrom(cols[j]));
                    valueBuilder.addValues(columnValueBuilder.build());
                }
                Map<String, SinkProto.ColumnValue> valueMap = valueBuilder.getValuesList()
                        .stream().collect(Collectors.toMap(SinkProto.ColumnValue::getName, v -> v));

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

                RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder()
                        .addColValues(ByteString.copyFrom(cols[0]))
                        .addColValues(ByteString.copyFrom(cols[1]))
                        .addColValues(ByteString.copyFrom(cols[2]))
                        .addColValues(ByteString.copyFrom(cols[3]))
                        .addIndexKeys(indexKey);

                tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
                tableUpdateData.add(tableUpdateDataBuilder.build());
                streamHandle.updateRecord(schemaName, tableUpdateData, 0);
            }
        }
    }
}
