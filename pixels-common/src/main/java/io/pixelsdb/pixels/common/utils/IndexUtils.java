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

 import io.pixelsdb.pixels.common.exception.MetadataException;
 import io.pixelsdb.pixels.common.metadata.MetadataService;
 import io.pixelsdb.pixels.common.metadata.domain.Column;
 import io.pixelsdb.pixels.common.metadata.domain.Schema;
 import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
 import io.pixelsdb.pixels.common.metadata.domain.Table;

 import java.util.LinkedList;
 import java.util.List;
 import java.util.stream.IntStream;

 public class IndexUtils
{
    private static final MetadataService metadataService = MetadataService.Instance();

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
}
