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
package io.pixelsdb.pixels.core.predicate;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created at: 07/04/2022
 * Author: hank
 */
public class TestPredicate
{
    @Test
    public void testJavaType()
    {
        assert int.class == Integer.TYPE;
        assert boolean.class == Boolean.TYPE;
    }

    @Test
    public void testSerializationAndDeserialization()
    {
        Filter<Long> longFilter = new Filter<>(Long.TYPE, false, false, false);
        longFilter.addRange(new Bound<>(Bound.Type.UNBOUNDED, null),
                new Bound<>(Bound.Type.INCLUDED, 100L));
        longFilter.addRange(new Bound<>(Bound.Type.EXCLUDED, 200L),
                new Bound<>(Bound.Type.UNBOUNDED, null));
        longFilter.addDiscreteValue(new Bound<>(Bound.Type.INCLUDED, 150L));
        ColumnFilter<Long> columnFilter = new ColumnFilter<>("id", TypeDescription.Category.LONG, longFilter);

        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(0, columnFilter);

        TableScanFilters tableScanFilters = new TableScanFilters("tpch", "orders", columnFilters);

        String json = JSON.toJSONString(tableScanFilters);

        System.out.println(json);

        TableScanFilters tableScanFilters1 = JSON.parseObject(json, TableScanFilters.class);
        ColumnFilter columnFilter1 = tableScanFilters1.getColumnFilter(0);
        System.out.println(columnFilter1.getColumnName());
        System.out.println(columnFilter1.getColumnType());
        System.out.println(columnFilter1.getFilter().getJavaType());
        System.out.println(columnFilter1.getFilter().getRangeCount());
        System.out.println(columnFilter1.getFilter().getDiscreteValueCount());
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

        Filter<Long> longFilter = new Filter<>(Long.TYPE, false, false, false);
        longFilter.addRange(new Bound<>(Bound.Type.UNBOUNDED, null),
                new Bound<>(Bound.Type.INCLUDED, 100L));
        longFilter.addRange(new Bound<>(Bound.Type.EXCLUDED, 200L),
                new Bound<>(Bound.Type.UNBOUNDED, null));
        ColumnFilter<Long> columnFilter = new ColumnFilter<>("o_orderkey", TypeDescription.Category.LONG, longFilter);
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(1, columnFilter);
        TableScanFilters tableScanFilters = new TableScanFilters("tpch", "orders", columnFilters);

        Bitmap filtered = new Bitmap(1024, true);
        Bitmap tmp = new Bitmap(1024, false);

        while (true)
        {
            VectorizedRowBatch rowBatch = recordReader.readBatch(1024);
            tableScanFilters.doFilter(rowBatch, filtered, tmp);
            if (filtered.cardinality() != 1024)
            {
                LongColumnVector columnVector = (LongColumnVector) rowBatch.cols[1];
                for (int i = 0; i < columnVector.getLength(); ++i)
                {
                    System.out.println(columnVector.vector[i]);
                }
            }
            rowBatch.applyFilter(filtered);
            if (rowBatch.size != 1024)
            {
                System.out.println("filtered row batch size: " + rowBatch.size);
                LongColumnVector columnVector = (LongColumnVector) rowBatch.cols[1];
                for (int i = 0; i < columnVector.getLength(); ++i)
                {
                    System.out.println(columnVector.vector[i]);
                }
            }
            if (rowBatch.endOfFile)
            {
                break;
            }
        }

        recordReader.close();
        pixelsReader.close();
    }
}
