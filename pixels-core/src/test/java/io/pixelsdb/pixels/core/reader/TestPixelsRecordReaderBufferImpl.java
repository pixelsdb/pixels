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

package io.pixelsdb.pixels.core.reader;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.retina.RetinaProto;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class TestPixelsRecordReaderBufferImpl
{
    @Test
    public void testReadBatch() throws RetinaException, IOException, TransException, MetadataException
    {

        Storage storage = StorageFactory.Instance().getStorage("minio");
        String schemaName = "pixels_bench_sf10x";
        String tableName = "checking";

        MetadataService metadataService = MetadataService.Instance();
        List<Column> columns = metadataService.getColumns(schemaName, tableName, false);
        Table table = metadataService.getTable(schemaName, tableName);
        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        List<String> columnTypes = columns.stream().map(Column::getType).collect(Collectors.toList());
        TypeDescription schema = TypeDescription.createSchemaFromStrings(columnNames, columnTypes);
        String[] includeCols = new String[columns.size()];
        for (int i = 0; i < includeCols.length; ++i)
        {
            includeCols[i] = columns.get(i).getName();
        }

        TransService transService = TransService.Instance();
        TransContext transContext = transService.beginTrans(true);
        long timeStamp = transContext.getTimestamp();
        long transId = transContext.getTransId();

        RetinaService retinaService = RetinaService.Instance();
        assert retinaService.isEnabled();
        RetinaProto.GetWriteBufferResponse superVersion = retinaService.getWriteBuffer(schemaName, tableName, timeStamp);

        PixelsReaderOption option = new PixelsReaderOption();

        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.enableEncodedColumnVector(true);
        option.readIntColumnAsIntVector(true);
        option.includeCols(includeCols);
        option.transId(transId);
        option.transTimestamp(timeStamp);
        String retinaServiceHost =  ConfigFactory.Instance().getProperty("retina.server.host");
        byte[] activeMemtableData = superVersion.getData().toByteArray();
        PixelsRecordReaderBufferImpl reader = new PixelsRecordReaderBufferImpl(
                option,
                retinaServiceHost,
                activeMemtableData, superVersion.getIdsList(),
                superVersion.getBitmapsList(),
                storage,
                table.getId(),
                schema
        );


        int exceptedBatchNum = superVersion.getIdsList().size() + 1;
        int readBatchNum = 0;
        while(true)
        {
            VectorizedRowBatch vectorizedRowBatch = reader.readBatch();
            if(vectorizedRowBatch.endOfFile)
            {
                break;
            }

            if(vectorizedRowBatch.isEmpty())
            {
                continue;
            }
            ++readBatchNum;
        }
        Assertions.assertEquals(exceptedBatchNum, readBatchNum);
    }
}
