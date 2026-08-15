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
package io.pixelsdb.pixels.example.core;

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.encoding.EncodingLevel;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Self-contained example for {@link PixelsReaderOption#rgRange(int, int)}.
 * It writes four row groups to a temporary local file and verifies whole-file,
 * single-row-group, and multi-row-group ranges.
 */
public class TestPixelsReaderOption
{
    private static final String SCHEMA_STRING = "struct<x:int,y:int>";
    private static final int ROWS_PER_GROUP = 8;
    private static final int ROW_GROUP_COUNT = 4;

    public static void main(String[] args) throws Exception
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        Path path = Files.createTempFile("pixels-reader-option-", ".pxl");
        Files.deleteIfExists(path);

        try
        {
            writeFile(storage, path.toString());
            try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(path.toString())
                    .setEnableCache(false)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build())
            {
                check(reader.getRowGroupNum() >= ROW_GROUP_COUNT,
                        "expected at least " + ROW_GROUP_COUNT + " row groups, got " +
                                reader.getRowGroupNum());

                assertRange(reader, 0, ROW_GROUP_COUNT, 0, ROWS_PER_GROUP * ROW_GROUP_COUNT);
                assertRange(reader, 0, 1, 0, ROWS_PER_GROUP);
                assertRange(reader, 1, 2, ROWS_PER_GROUP, ROWS_PER_GROUP * 2);
                assertRange(reader, 2, 2, ROWS_PER_GROUP * 2, ROWS_PER_GROUP * 2);
            }

            System.out.println("PixelsReaderOption rgRange example passed.");
        }
        finally
        {
            Files.deleteIfExists(path);
        }
    }

    private static void writeFile(Storage storage, String path) throws Exception
    {
        TypeDescription schema = TypeDescription.fromString(SCHEMA_STRING);
        try (PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(ROWS_PER_GROUP)
                .setRowGroupSize(1)
                .setStorage(storage)
                .setPath(path)
                .setBlockSize(1024 * 1024)
                .setReplication((short) 1)
                .setBlockPadding(false)
                .setOverwrite(true)
                .setEncodingLevel(EncodingLevel.EL0)
                .setCompressionBlockSize(1)
                .setNullsPadding(false)
                .build())
        {
            for (int group = 0; group < ROW_GROUP_COUNT; ++group)
            {
                VectorizedRowBatch batch = schema.createRowBatch(ROWS_PER_GROUP);
                IntColumnVector x = (IntColumnVector) batch.cols[0];
                IntColumnVector y = (IntColumnVector) batch.cols[1];
                for (int row = 0; row < ROWS_PER_GROUP; ++row)
                {
                    int globalRow = group * ROWS_PER_GROUP + row;
                    x.add(globalRow);
                    y.add(globalRow * 10);
                }
                batch.size = ROWS_PER_GROUP;
                writer.addRowBatch(batch);
            }
        }
    }

    private static void assertRange(PixelsReader reader, int rgStart, int rgLen,
                                    int expectedStart, int expectedRows) throws Exception
    {
        PixelsReaderOption option = new PixelsReaderOption()
                .includeCols(new String[]{"x", "y"})
                .rgRange(rgStart, rgLen)
                .skipCorruptRecords(true)
                .tolerantSchemaEvolution(true);

        int rowsRead = 0;
        try (PixelsRecordReader recordReader = reader.read(option))
        {
            while (true)
            {
                VectorizedRowBatch batch = recordReader.readBatch(7);
                IntColumnVector x = (IntColumnVector) batch.cols[0];
                IntColumnVector y = (IntColumnVector) batch.cols[1];
                for (int row = 0; row < batch.size; ++row)
                {
                    int expected = expectedStart + rowsRead;
                    check(x.vector[row] == expected,
                            "x mismatch for rgRange(" + rgStart + ", " + rgLen + ") at row " +
                                    rowsRead + ": expected " + expected + ", got " + x.vector[row]);
                    check(y.vector[row] == expected * 10,
                            "y mismatch for rgRange(" + rgStart + ", " + rgLen + ") at row " +
                                    rowsRead + ": expected " + expected * 10 + ", got " + y.vector[row]);
                    rowsRead++;
                }
                if (batch.endOfFile)
                {
                    break;
                }
            }
        }
        check(rowsRead == expectedRows,
                "row count mismatch for rgRange(" + rgStart + ", " + rgLen + "): expected " +
                        expectedRows + ", got " + rowsRead);
    }

    private static void check(boolean condition, String message)
    {
        if (!condition)
        {
            throw new AssertionError(message);
        }
    }
}
