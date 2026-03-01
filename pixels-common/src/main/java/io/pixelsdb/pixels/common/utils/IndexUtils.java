 /*
  * Copyright 2026 PixelsDB.
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

 package io.pixelsdb.pixels.common.utils;

 import com.google.common.hash.Hashing;
 import com.google.protobuf.ByteString;
 import io.pixelsdb.pixels.common.exception.MetadataException;
 import io.pixelsdb.pixels.common.exception.SinglePointIndexException;
 import io.pixelsdb.pixels.common.metadata.MetadataService;
 import io.pixelsdb.pixels.common.metadata.domain.Column;
 import io.pixelsdb.pixels.common.metadata.domain.Schema;
 import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
 import io.pixelsdb.pixels.common.metadata.domain.Table;

 import java.util.LinkedList;
 import java.util.List;
 import java.util.Arrays;
 import java.util.stream.IntStream;
 import java.nio.charset.StandardCharsets;

 public class IndexUtils
{
    private static final MetadataService metadataService = MetadataService.Instance();
    private static volatile IndexUtils instance;
    private final int bucketNum;
    private static final Integer VARIABLE_LEN_SENTINEL = -2;
    private static final String defaultColumnFamily = "default";

    private IndexUtils()
    {
        ConfigFactory config = ConfigFactory.Instance();
        this.bucketNum = Integer.parseInt(config.getProperty("index.bucket.num"));
    }

    public static IndexUtils getInstance()
    {
        if (instance == null)
        {
            synchronized (IndexUtils.class)
            {
                if (instance == null)
                {
                    instance = new IndexUtils();
                }
            }
        }
        return instance;
    }

    public static List<Column> extractInfoFromIndex(long tableId, long indexId) throws MetadataException
    {
        Table table = metadataService.getTableById(tableId);
        Schema schema = metadataService.getSchemaById(table.getSchemaId());
        return extractInfoFromIndex(schema.getName(), table.getName(), indexId);
    }

    public static List<Column> extractInfoFromIndex(String dbName, String tableName, long indexId) throws MetadataException
    {
        List<Column> columns = metadataService.getColumns(dbName, tableName, false);
        SinglePointIndex index = metadataService.getSinglePointIndex(indexId);

        int[] orderKeyColIds = new int[index.getKeyColumns().getKeyColumnIds().size()];
        List<Column> orderedKeyCols = new LinkedList<>();
        int keyColumnIdx = 0;
        for (Integer keyColumnId : index.getKeyColumns().getKeyColumnIds()) {
            int i = IntStream.range(0, columns.size())
                    .filter(idx -> columns.get(idx).getId() == keyColumnId)
                    .findFirst()
                    .orElse(-1);
            if(i == -1)
            {
                throw new MetadataException("Cant find key column id: " + keyColumnId + " in table "
                        + tableName + " schema id is " + dbName);
            }
            orderKeyColIds[keyColumnIdx++] = i;
            orderedKeyCols.add(columns.get(i));
        }
        return orderedKeyCols;
    }

    public static int getBucketIdFromByteBuffer(ByteString byteString)
    {
        IndexUtils indexUtils = IndexUtils.getInstance();

        int hash = Hashing.sha256()
                .hashBytes(byteString.toByteArray())
                .asInt();

        int absHash = Math.abs(hash);

        return absHash % indexUtils.getBucketNum();
    }

    public int getBucketNum()
    {
        return bucketNum;
    }

    public static String getCFName(long tableId, long indexId, int vNodeId, boolean multiCF) 
    {
        if(multiCF)
        {
            return "t" + tableId + "_i" + indexId + "_v" + vNodeId;
        }
        else
        {
            return defaultColumnFamily;
        }
    }

    public static int keyLengthOf(Class<?> clazz) 
    {
        if (clazz == boolean.class) return 1;
        if (clazz == byte.class)    return 1;
        if (clazz == short.class)   return 2;
        if (clazz == int.class)     return 4;
        if (clazz == long.class)    return 8;
        if (clazz == float.class)   return 4;
        if (clazz == double.class)  return 8;
        return -1;
    }

    public static long[] parseTableAndIndexId(byte[] cfNameBytes) throws SinglePointIndexException
    {
        if (cfNameBytes == null || Arrays.equals(cfNameBytes, "default".getBytes(StandardCharsets.UTF_8)))
        {
            return null;
        }

        String name = new String(cfNameBytes, StandardCharsets.UTF_8);

        try
        {
            // Expected format: "t{tableId}_i{indexId}_v{vNodeId}"
            // Example: "t100_i200_v5" -> ["100", "200", "30"]
            if (name.startsWith("t") && name.contains("_i") && name.contains("_v"))
            {
                // Remove the leading 't'
                String content = name.substring(1);

                // Split using regex for multiple delimiters: _i and _v
                String[] parts = content.split("_i|_v");

                if (parts.length == 3)
                {
                    long tableId = Long.parseLong(parts[0]);
                    long indexId = Long.parseLong(parts[1]);
                    long vNodeId = Long.parseLong(parts[2]);

                    return new long[]{tableId, indexId, vNodeId};
                }
                else
                {
                    throw new SinglePointIndexException("Failed to parse CF name (invalid segments): " + name);
                }
            }
        }
        catch (Exception e)
        {
            throw new SinglePointIndexException("Failed to parse CF name: " + name, e);
        }

        return null;
    }
}
