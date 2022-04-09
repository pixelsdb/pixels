/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.lambda;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.predicate.ColumnFilter;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;

/**
 * Created at: 07/04/2022
 * Author: hank
 */
public class TestColumnFilter
{
    @Test
    public void testParsing()
    {
        long start = System.currentTimeMillis();
        String json = "{\"columnName\":\"id\",\"columnType\":\"LONG\",\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"lowerBounds\\\":[{\\\"type\\\":\\\"UNBOUNDED\\\"},{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}],\\\"upperBounds\\\":[{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100},{\\\"type\\\":\\\"UNBOUNDED\\\"}],\\\"singleValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":150}]}\"}\n";
        ColumnFilter filter = new Gson().fromJson(json, ColumnFilter.class);
        System.out.println(System.currentTimeMillis()-start);
        ColumnFilter filter0 = JSON.parseObject(json, ColumnFilter.class);
        System.out.println(System.currentTimeMillis()-start);
    }

    @Test
    public void testFilter() throws IOException
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        PixelsReader pixelsReader = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setEnableCache(false)
                .setPixelsFooterCache(new PixelsFooterCache())
                .setPath("file:///home/hank/20220312072707_0.pxl").build();
        PixelsReaderOption readerOption = new PixelsReaderOption();
        readerOption.queryId(1);
        readerOption.rgRange(0, 1);
        readerOption.includeCols(new String[] {"o_custkey", "o_orderkey", "o_orderdate"});
        PixelsRecordReader recordReader = pixelsReader.read(readerOption);

        ExprTree bigintFilter1 = new ExprTree("o_orderkey", ExprTree.Operator.LE, "100");
        ExprTree bigintFilter2 = new ExprTree("o_orderkey", ExprTree.Operator.GT, "200");
        ExprTree filter = new ExprTree(ExprTree.Operator.OR, bigintFilter1, bigintFilter2);

        TypeDescription rowBatchSchema = recordReader.getResultSchema();
        filter.prepare(rowBatchSchema);

        while (true)
        {
            VectorizedRowBatch rowBatch = recordReader.readBatch(1024);
            VectorizedRowBatch newRowBatch = filter.filter(rowBatch, rowBatchSchema.createRowBatch(1024));
            System.out.println(newRowBatch.size);
            if (rowBatch.endOfFile)
            {
                break;
            }
        }

        recordReader.close();
        pixelsReader.close();
    }
}
