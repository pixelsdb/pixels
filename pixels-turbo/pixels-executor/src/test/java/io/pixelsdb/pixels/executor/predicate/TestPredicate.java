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
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.utils.Bitmap;
import io.pixelsdb.pixels.core.utils.Decimal;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    public void testLongFilterSerDe()
    {
        Filter<Long> longFilter = new Filter<>(Long.TYPE, false, false, false, false);
        longFilter.addRange(new Bound<>(Bound.Type.UNBOUNDED, null),
                new Bound<>(Bound.Type.INCLUDED, 100L));
        longFilter.addRange(new Bound<>(Bound.Type.EXCLUDED, 200L),
                new Bound<>(Bound.Type.UNBOUNDED, null));
        longFilter.addDiscreteValue(new Bound<>(Bound.Type.INCLUDED, 150L));
        ColumnFilter<Long> columnFilter = new ColumnFilter<>("id", TypeDescription.Category.LONG, longFilter);

        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(0, columnFilter);

        TableScanFilter tableScanFilter = new TableScanFilter("tpch", "orders", columnFilters);

        String json = JSON.toJSONString(tableScanFilter);

        System.out.println(json);

        TableScanFilter tableScanFilter1 = JSON.parseObject(json, TableScanFilter.class);
        ColumnFilter columnFilter1 = tableScanFilter1.getColumnFilter(0);
        System.out.println(columnFilter1.getColumnName());
        System.out.println(columnFilter1.getColumnType());
        System.out.println(columnFilter1.getFilter().getJavaType());
        System.out.println(columnFilter1.getFilter().getRangeCount());
        System.out.println(columnFilter1.getFilter().getDiscreteValueCount());
    }

    @Test
    public void testStringFilterSerDe()
    {
        Filter<String> stringFilter = new Filter<>(String.class, false, false, false, false);
        stringFilter.addRange(new Bound<>(Bound.Type.UNBOUNDED, null),
                new Bound<>(Bound.Type.INCLUDED, "123"));
        stringFilter.addRange(new Bound<>(Bound.Type.EXCLUDED, "456"),
                new Bound<>(Bound.Type.UNBOUNDED, null));
        stringFilter.addDiscreteValue(new Bound<>(Bound.Type.INCLUDED, "789"));
        ColumnFilter<String> columnFilter = new ColumnFilter<>("id", TypeDescription.Category.STRING, stringFilter);

        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(0, columnFilter);

        TableScanFilter tableScanFilter = new TableScanFilter("tpch", "orders", columnFilters);

        String json = JSON.toJSONString(tableScanFilter);

        System.out.println(json);

        TableScanFilter tableScanFilter1 = JSON.parseObject(json, TableScanFilter.class);
        ColumnFilter columnFilter1 = tableScanFilter1.getColumnFilter(0);
        System.out.println(columnFilter1.getColumnName());
        System.out.println(columnFilter1.getColumnType());
        System.out.println(columnFilter1.getFilter().getJavaType());
        System.out.println(columnFilter1.getFilter().getRangeCount());
        System.out.println(columnFilter1.getFilter().getDiscreteValueCount());
    }

    @Test
    public void testDecimalFilterSerDe()
    {
        Filter<Decimal> decimalFilter = new Filter<>(Decimal.class, false, false, false, false);
        decimalFilter.addRange(new Bound<>(Bound.Type.UNBOUNDED, null),
                new Bound<>(Bound.Type.INCLUDED, new Decimal(1234, 15, 2)));
        decimalFilter.addRange(new Bound<>(Bound.Type.EXCLUDED, new Decimal(4567, 15, 2)),
                new Bound<>(Bound.Type.UNBOUNDED, null));
        decimalFilter.addDiscreteValue(new Bound<>(Bound.Type.INCLUDED, new Decimal(7890, 15, 2)));
        ColumnFilter<Decimal> columnFilter = new ColumnFilter<>("id", TypeDescription.Category.DECIMAL, decimalFilter);

        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(0, columnFilter);

        TableScanFilter tableScanFilter = new TableScanFilter("tpch", "orders", columnFilters);

        String json = JSON.toJSONString(tableScanFilter);

        System.out.println(json);

        TableScanFilter tableScanFilter1 = JSON.parseObject(json, TableScanFilter.class);
        ColumnFilter columnFilter1 = tableScanFilter1.getColumnFilter(0);
        System.out.println(columnFilter1.getColumnName());
        System.out.println(columnFilter1.getColumnType());
        System.out.println(columnFilter1.getFilter().getJavaType());
        System.out.println(columnFilter1.getFilter().getRangeCount());
        System.out.println(columnFilter1.getFilter().getDiscreteValueCount());
    }

    private static final int pixelStride = 10000;
    private static final int rowGroupSize = 256 * 1024 * 1024;
    private static final long blockSize = 2048L * 1024L * 1024L;
    private static final short replication = (short) 1;

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
        readerOption.includeCols(new String[] {"o_custkey", "o_orderkey", "o_orderdate", "o_orderstatus"});
        PixelsRecordReader recordReader = pixelsReader.read(readerOption);

        Filter<Long> longFilter = new Filter<>(Long.TYPE, false, false, false, false);
        longFilter.addRange(new Bound<>(Bound.Type.UNBOUNDED, null),
                new Bound<>(Bound.Type.INCLUDED, 100L));
        longFilter.addRange(new Bound<>(Bound.Type.EXCLUDED, 200L),
                new Bound<>(Bound.Type.UNBOUNDED, null));
        ColumnFilter<Long> columnFilter = new ColumnFilter<>("o_orderkey", TypeDescription.Category.LONG, longFilter);
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(1, columnFilter);
        TableScanFilter tableScanFilter = new TableScanFilter("tpch", "orders", columnFilters);

        Bitmap filtered = new Bitmap(102400, true);
        Bitmap tmp = new Bitmap(102400, false);

        PixelsWriter pixelsWriter = PixelsWriterImpl.newBuilder()
                .setSchema(recordReader.getResultSchema())
                .setPixelStride(pixelStride)
                .setRowGroupSize(rowGroupSize)
                .setStorage(storage)
                .setPath("file:///home/hank/result_0.pxl")
                .setBlockSize(blockSize)
                .setReplication(replication)
                .setBlockPadding(true)
                .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncoding(true) // it is worth to do encoding
                .setCompressionBlockSize(1)
                .build();

        while (true)
        {
            VectorizedRowBatch rowBatch = recordReader.readBatch(102400);
            tableScanFilter.doFilter(rowBatch, filtered, tmp);
            System.out.println(rowBatch.size + ", " + filtered.cardinality(0, rowBatch.size));
            rowBatch.applyFilter(filtered);
            if (rowBatch.size > 0)
            {
                pixelsWriter.addRowBatch(rowBatch);
            }
            if (rowBatch.endOfFile)
            {
                break;
            }
        }

        recordReader.close();
        pixelsReader.close();
        pixelsWriter.close();
    }

    @Test
    public void testStringDecode()
    {
        String str = "hello world";
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        assert bytes.length == 11;
        bytes = str.getBytes(StandardCharsets.ISO_8859_1);
        assert bytes.length == 11;
        str = "你好世界";
        bytes = str.getBytes(StandardCharsets.UTF_8);
        System.out.println(bytes.length);
        for (byte b : bytes)
        {
            System.out.println(b);
        }
    }

    @Test
    public void testParseDiscrete()
    {
        String json = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\"," +
                "\"columnFilters\":{1:{\"columnName\":\"c_mktsegment\"," +
                "\"columnType\":\"CHAR\",\"filterJson\":\"{\\\"javaType\\\":" +
                "\\\"java.lang.String\\\",\\\"isAll\\\":false,\\\"isNone\\\":false," +
                "\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[]," +
                "\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\"," +
                "\\\"value\\\":\\\"BUILDING\\\"}]}\"}}}";

        TableScanFilter filter = JSON.parseObject(json, TableScanFilter.class);
        assert filter.getColumnFilter(1).getFilter().getRangeCount() == 0;
        assert filter.getColumnFilter(1).getFilter().getDiscreteValueCount() == 1;
    }

    @Test
    public void testParseEmptyFilter()
    {
        String json = "{\"schemaName\":\"tpch\",\"tableName\":\"part\",\"columnFilters\":{}}";
        TableScanFilter filter = JSON.parseObject(json, TableScanFilter.class);
        assert filter.getColumnFilters().isEmpty();
    }
}
