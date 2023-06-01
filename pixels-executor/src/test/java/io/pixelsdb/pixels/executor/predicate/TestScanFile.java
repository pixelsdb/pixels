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
package io.pixelsdb.pixels.executor.predicate;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.List;

/**
 * @author hank
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestScanFile
{

    @Test
    public void testScanFile()
    {
        String path = "file:///home/hank/Desktop/orders_part_0";
        PixelsReader reader;
        try
        {
            String filter =
                    "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                            "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
                            "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                            "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                            "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                            "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                            "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                            "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                            "\\\"discreteValues\\\":[]}\"}}}";
            TableScanFilter tableScanFilter = JSON.parseObject(filter, TableScanFilter.class);
            Storage storage = StorageFactory.Instance().getStorage("file");
            reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(path)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
            List<PixelsProto.Type> types = reader.getFooter().getTypesList();
            for (PixelsProto.Type type : types)
            {
                System.out.println(type);
            }
            System.out.println(reader.getRowGroupStats().size());

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.transId(123456);
            option.includeCols(new String[] {"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
            option.rgRange(0, 40);
            PixelsRecordReader recordReader = reader.read(option);
            int rowNum = 0;
            Bitmap filtered = new Bitmap(1024, true);
            Bitmap tmp = new Bitmap(1024, false);
            while (true)
            {
                VectorizedRowBatch rowBatch = recordReader.readBatch(1024);
                tableScanFilter.doFilter(rowBatch, filtered, tmp);
                rowBatch.applyFilter(filtered);
                rowNum += rowBatch.size;
                //int[] hashCode = new int[rowBatch.size];
                //Arrays.fill(hashCode, 0);
                //rowBatch.cols[0].accumulateHashCode(hashCode);

                if (rowBatch.endOfFile)
                {
                    break;
                }
            }
            System.out.println(rowNum);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}