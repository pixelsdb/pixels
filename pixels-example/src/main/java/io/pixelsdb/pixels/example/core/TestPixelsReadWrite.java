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
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.vector.DecimalColumnVector;
import io.pixelsdb.pixels.core.vector.DoubleColumnVector;
import io.pixelsdb.pixels.core.vector.FloatColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.ShortColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * End-to-end example and self-check that writes every supported column type to a local
 * Pixels file and reads it back, verifying each value (including nulls) round-trips.
 * <p>
 * This single class exercises the writer, the reader, and all column vectors together.
 * The file is written across multiple row groups and read back with a batch size that
 * crosses row-group boundaries, so multi-row-group stride handling is covered as well.
 * <p>
 * For minimal, single-purpose read/write snippets, see {@code TestPixelsReader} and
 * {@code TestPixelsWriter}.
 */
public class TestPixelsReadWrite
{
    private static final String SCHEMA_STRING =
            "struct<c_bool:boolean,c_short:smallint,c_int:int,c_long:bigint,"
                    + "c_float:float,c_double:double,c_dec:decimal(12,3),"
                    + "c_date:date,c_time:time,c_ts:timestamp,c_str:string>";

    private static final int ROWS_PER_GROUP = 10;
    private static final int GROUP_COUNT = 3;
    private static final int TOTAL_ROWS = ROWS_PER_GROUP * GROUP_COUNT;
    private static final int READ_BATCH_SIZE = 7; // odd size to cross row-group boundaries

    public static void main(String[] args) throws Exception
    {
        Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
        Path path = Files.createTempFile("pixels-read-write-", ".pxl");
        Files.deleteIfExists(path);
        try
        {
            writeFile(storage, path.toString());
            readAndVerify(storage, path.toString());
            System.out.println("TestPixelsReadWrite passed: " + TOTAL_ROWS + " rows round-tripped across "
                    + GROUP_COUNT + " row groups.");
        }
        finally
        {
            Files.deleteIfExists(path);
        }
    }

    // ---- deterministic expected values (single source of truth for write and verify) ----

    private static boolean isNullRow(int g)
    {
        return g % 6 == 5; // rows 5, 11, 17, 23, 29 are null
    }

    private static byte expectedBool(int g)  { return (byte) (g % 2); }
    private static short expectedShort(int g) { return (short) (g - 15); }
    private static int expectedInt(int g)     { return g * 1000; }
    private static long expectedLong(int g)   { return (long) g * 1_000_000L; }
    private static float expectedFloat(int g) { return g * 1.5f; }
    private static double expectedDouble(int g) { return g * 2.25d; }
    private static long expectedDecimalUnscaled(int g) { return g * 1000L + 123L; } // decimal(12,3)
    private static int expectedDate(int g)    { return 19000 + g; } // days since epoch
    private static int expectedTimeMillis(int g) { return g * 1000; } // millis in day, precision 3
    private static long expectedTsMicros(int g)  { return (long) g * 1_000_000L; } // whole seconds, precision 3 safe
    private static String expectedStr(int g)  { return "s" + g; }

    // ------------------------------- write -------------------------------

    private static void writeFile(Storage storage, String filePath) throws Exception
    {
        TypeDescription schema = TypeDescription.fromString(SCHEMA_STRING);
        PixelsWriter writer = PixelsWriterImpl.newBuilder()
                .setSchema(schema)
                .setPixelStride(ROWS_PER_GROUP)
                .setRowGroupSize(1) // minimal → forces each added batch into its own row group
                .setStorage(storage)
                .setPath(filePath)
                .setBlockSize(256 * 1024)
                .setReplication((short) 1)
                .setBlockPadding(false)
                .setEncodingLevel(EncodingLevel.EL2)
                .setCompressionBlockSize(1)
                .setNullsPadding(true)
                .build();

        for (int group = 0; group < GROUP_COUNT; group++)
        {
            VectorizedRowBatch rowBatch = schema.createRowBatch(ROWS_PER_GROUP);
            ByteColumnVector cBool = (ByteColumnVector) rowBatch.cols[0];
            ShortColumnVector cShort = (ShortColumnVector) rowBatch.cols[1];
            IntColumnVector cInt = (IntColumnVector) rowBatch.cols[2];
            LongColumnVector cLong = (LongColumnVector) rowBatch.cols[3];
            FloatColumnVector cFloat = (FloatColumnVector) rowBatch.cols[4];
            DoubleColumnVector cDouble = (DoubleColumnVector) rowBatch.cols[5];
            DecimalColumnVector cDec = (DecimalColumnVector) rowBatch.cols[6];
            DateColumnVector cDate = (DateColumnVector) rowBatch.cols[7];
            TimeColumnVector cTime = (TimeColumnVector) rowBatch.cols[8];
            TimestampColumnVector cTs = (TimestampColumnVector) rowBatch.cols[9];
            BinaryColumnVector cStr = (BinaryColumnVector) rowBatch.cols[10];

            for (int i = 0; i < ROWS_PER_GROUP; i++)
            {
                int g = group * ROWS_PER_GROUP + i;
                int row = rowBatch.size++;
                if (isNullRow(g))
                {
                    cBool.isNull[row] = true;   cBool.noNulls = false;
                    cShort.isNull[row] = true;  cShort.noNulls = false;
                    cInt.isNull[row] = true;    cInt.noNulls = false;
                    cLong.isNull[row] = true;   cLong.noNulls = false;
                    cFloat.isNull[row] = true;  cFloat.noNulls = false;
                    cDouble.isNull[row] = true; cDouble.noNulls = false;
                    cDec.isNull[row] = true;    cDec.noNulls = false;
                    cDate.isNull[row] = true;   cDate.noNulls = false;
                    cTime.isNull[row] = true;   cTime.noNulls = false;
                    cTs.isNull[row] = true;     cTs.noNulls = false;
                    cStr.isNull[row] = true;    cStr.noNulls = false;
                    continue;
                }
                cBool.vector[row] = expectedBool(g);          cBool.isNull[row] = false;
                cShort.vector[row] = expectedShort(g);        cShort.isNull[row] = false;
                cInt.vector[row] = expectedInt(g);            cInt.isNull[row] = false;
                cLong.vector[row] = expectedLong(g);          cLong.isNull[row] = false;
                cFloat.vector[row] = Float.floatToIntBits(expectedFloat(g));    cFloat.isNull[row] = false;
                cDouble.vector[row] = Double.doubleToLongBits(expectedDouble(g)); cDouble.isNull[row] = false;
                cDec.vector[row] = expectedDecimalUnscaled(g); cDec.isNull[row] = false;
                cDate.set(row, expectedDate(g));
                cTime.set(row, expectedTimeMillis(g));
                cTs.set(row, expectedTsMicros(g));
                cStr.setVal(row, expectedStr(g).getBytes());  cStr.isNull[row] = false;
            }
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        }
        writer.close();
    }

    // ------------------------------- read + verify -------------------------------

    private static void readAndVerify(Storage storage, String filePath) throws Exception
    {
        try (PixelsReader reader = PixelsReaderImpl.newBuilder()
                .setStorage(storage)
                .setPath(filePath)
                .setPixelsFooterCache(new PixelsFooterCache())
                .build())
        {
            check(reader.getRowGroupNum() == GROUP_COUNT,
                    "expected " + GROUP_COUNT + " row groups, got " + reader.getRowGroupNum());

            PixelsReaderOption option = new PixelsReaderOption();
            option.skipCorruptRecords(true);
            option.tolerantSchemaEvolution(true);
            option.includeCols(reader.getFileSchema().getFieldNames().toArray(new String[0]));

            PixelsRecordReader recordReader = reader.read(option);
            int totalRead = 0;
            while (true)
            {
                VectorizedRowBatch batch = recordReader.readBatch(READ_BATCH_SIZE);
                if (batch.size == 0 && batch.endOfFile)
                {
                    break;
                }
                verifyBatch(batch, totalRead);
                totalRead += batch.size;
                if (batch.endOfFile)
                {
                    break;
                }
            }
            recordReader.close();
            check(totalRead == TOTAL_ROWS, "expected " + TOTAL_ROWS + " rows, got " + totalRead);
        }
    }

    private static void verifyBatch(VectorizedRowBatch batch, int rowOffset)
    {
        ByteColumnVector cBool = (ByteColumnVector) batch.cols[0];
        ShortColumnVector cShort = (ShortColumnVector) batch.cols[1];
        IntColumnVector cInt = (IntColumnVector) batch.cols[2];
        LongColumnVector cLong = (LongColumnVector) batch.cols[3];
        FloatColumnVector cFloat = (FloatColumnVector) batch.cols[4];
        DoubleColumnVector cDouble = (DoubleColumnVector) batch.cols[5];
        DecimalColumnVector cDec = (DecimalColumnVector) batch.cols[6];
        DateColumnVector cDate = (DateColumnVector) batch.cols[7];
        TimeColumnVector cTime = (TimeColumnVector) batch.cols[8];
        TimestampColumnVector cTs = (TimestampColumnVector) batch.cols[9];
        BinaryColumnVector cStr = (BinaryColumnVector) batch.cols[10];

        for (int i = 0; i < batch.size; i++)
        {
            int g = rowOffset + i;
            if (isNullRow(g))
            {
                check(cBool.isNull[i], "row " + g + " c_bool should be null");
                check(cShort.isNull[i], "row " + g + " c_short should be null");
                check(cInt.isNull[i], "row " + g + " c_int should be null");
                check(cLong.isNull[i], "row " + g + " c_long should be null");
                check(cFloat.isNull[i], "row " + g + " c_float should be null");
                check(cDouble.isNull[i], "row " + g + " c_double should be null");
                check(cDec.isNull[i], "row " + g + " c_dec should be null");
                check(cDate.isNull[i], "row " + g + " c_date should be null");
                check(cTime.isNull[i], "row " + g + " c_time should be null");
                check(cTs.isNull[i], "row " + g + " c_ts should be null");
                check(cStr.isNull[i], "row " + g + " c_str should be null");
                continue;
            }
            check(cBool.vector[i] == expectedBool(g), "row " + g + " c_bool");
            check(cShort.vector[i] == expectedShort(g), "row " + g + " c_short");
            check(cInt.vector[i] == expectedInt(g), "row " + g + " c_int");
            check(cLong.vector[i] == expectedLong(g), "row " + g + " c_long");
            check(Float.intBitsToFloat(cFloat.vector[i]) == expectedFloat(g), "row " + g + " c_float");
            check(Double.longBitsToDouble(cDouble.vector[i]) == expectedDouble(g), "row " + g + " c_double");
            check(cDec.vector[i] == expectedDecimalUnscaled(g), "row " + g + " c_dec");
            check(cDate.dates[i] == expectedDate(g), "row " + g + " c_date");
            check(cTime.times[i] == expectedTimeMillis(g), "row " + g + " c_time");
            check(cTs.times[i] == expectedTsMicros(g), "row " + g + " c_ts");
            check(expectedStr(g).equals(new String(cStr.vector[i], cStr.start[i], cStr.lens[i])),
                    "row " + g + " c_str");
        }
    }

    private static void check(boolean condition, String message)
    {
        if (!condition)
        {
            throw new AssertionError(message);
        }
    }
}
