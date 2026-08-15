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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ByteColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.DateColumnVector;
import io.pixelsdb.pixels.core.vector.IntColumnVector;
import io.pixelsdb.pixels.core.vector.LongColumnVector;
import io.pixelsdb.pixels.core.vector.ShortColumnVector;
import io.pixelsdb.pixels.core.vector.TimeColumnVector;
import io.pixelsdb.pixels.core.vector.TimestampColumnVector;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Covers type-string parsing and single-cell byte conversion for {@link TypeDescription}.
 */
public class TestTypeDescription
{
    @Test
    public void testParseDefaultsAndEntryPointsAgree()
    {
        assertEquals(TypeDescription.DEFAULT_VARCHAR_OR_BINARY_LENGTH,
                parseBoth(" varchar ").getMaxLength());
        assertEquals(TypeDescription.DEFAULT_VARCHAR_OR_BINARY_LENGTH,
                parseBoth("binary").getMaxLength());
        assertEquals(TypeDescription.DEFAULT_VARCHAR_OR_BINARY_LENGTH,
                parseBoth("varbinary").getMaxLength());
        assertEquals(TypeDescription.DEFAULT_CHAR_LENGTH,
                parseBoth("char").getMaxLength());
        assertEquals(TypeDescription.DEFAULT_TIME_PRECISION,
                parseBoth("time").getPrecision());
        assertEquals(TypeDescription.DEFAULT_TIMESTAMP_PRECISION,
                parseBoth("timestamp").getPrecision());

        TypeDescription decimalP = parseBoth("decimal(12)");
        assertEquals(12, decimalP.getPrecision());
        assertEquals(TypeDescription.DEFAULT_DECIMAL_SCALE, decimalP.getScale());

        TypeDescription decimalSpaced = parseBoth(" decimal ( 15 , 2 ) ");
        assertEquals(15, decimalSpaced.getPrecision());
        assertEquals(2, decimalSpaced.getScale());

        TypeDescription decimalDefault = parseBoth("decimal");
        assertEquals(TypeDescription.DEFAULT_LONG_DECIMAL_PRECISION, decimalDefault.getPrecision());
        assertEquals(TypeDescription.DEFAULT_DECIMAL_SCALE, decimalDefault.getScale());

        assertNull(TypeDescription.fromString(null));
    }

    @Test
    public void testParseTimePrecisionAndVectorAliases()
    {
        assertEquals(0, parseBoth("time(0)").getPrecision());
        assertEquals(1, parseBoth("time(1)").getPrecision());
        assertEquals(3, parseBoth("time(3)").getPrecision());
        assertEquals(6, parseBoth("timestamp(6)").getPrecision());
        assertIllegal("time(4)");
        assertIllegal("timestamp(7)");

        TypeDescription vector = parseBoth("vector(128)");
        assertEquals(TypeDescription.Category.VECTOR, vector.getCategory());
        assertEquals(128, vector.getDimension());

        TypeDescription array = parseBoth("array(double)");
        assertEquals(TypeDescription.Category.VECTOR, array.getCategory());
        assertEquals(TypeDescription.DEFAULT_VECTOR_DIMENSION, array.getDimension());

        // Mixed struct must not confuse global contains("array") / contains("vector").
        TypeDescription mixed = parseBoth("struct<v:vector(128),a:array(double)>");
        assertEquals(128, mixed.getChildren().get(0).getDimension());
        assertEquals(TypeDescription.DEFAULT_VECTOR_DIMENSION,
                mixed.getChildren().get(1).getDimension());
        assertIllegal("array(128)");
    }

    @Test
    public void testConvertSqlAndVectorAgree()
    {
        ByteColumnVector boolCol = new ByteColumnVector(2);
        boolCol.vector[0] = 1;
        boolCol.vector[1] = 0;
        assertConvert(TypeDescription.createBoolean(), boolCol, 0, "true", new byte[]{1});
        assertConvert(TypeDescription.createBoolean(), boolCol, 1, "false", new byte[]{0});

        ByteColumnVector byteCol = new ByteColumnVector(1);
        byteCol.vector[0] = -12;
        assertConvert(TypeDescription.createByte(), byteCol, 0, "-12", new byte[]{-12});

        ShortColumnVector shortCol = new ShortColumnVector(1);
        shortCol.vector[0] = -1234;
        assertConvert(TypeDescription.createShort(), shortCol, 0, "-1234",
                ByteBuffer.allocate(Short.BYTES).putShort((short) -1234).array());

        IntColumnVector intCol = new IntColumnVector(1);
        intCol.vector[0] = -1234;
        assertConvert(TypeDescription.createInt(), intCol, 0, "-1234",
                ByteBuffer.allocate(Integer.BYTES).putInt(-1234).array());

        LongColumnVector longCol = new LongColumnVector(1);
        longCol.vector[0] = Long.MAX_VALUE;
        assertConvert(TypeDescription.createLong(), longCol, 0, String.valueOf(Long.MAX_VALUE),
                ByteBuffer.allocate(Long.BYTES).putLong(Long.MAX_VALUE).array());

        DateColumnVector dateCol = new DateColumnVector(1);
        dateCol.dates[0] = 1;
        assertConvert(TypeDescription.createDate(), dateCol, 0, "1970-01-02",
                ByteBuffer.allocate(Integer.BYTES).putInt(1).array());

        TimeColumnVector timeCol = new TimeColumnVector(1, 3);
        timeCol.times[0] = 3723123;
        assertConvert(TypeDescription.createTime(3), timeCol, 0, "01:02:03.123",
                ByteBuffer.allocate(Integer.BYTES).putInt(3723123).array());

        TimestampColumnVector tsCol = new TimestampColumnVector(1, 6);
        tsCol.times[0] = 1234567L;
        assertConvert(TypeDescription.createTimestamp(6), tsCol, 0, "1970-01-01 00:00:01.234567",
                ByteBuffer.allocate(Long.BYTES).putLong(1234567L).array());

        BinaryColumnVector binaryCol = new BinaryColumnVector(1);
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        binaryCol.setVal(0, payload);
        assertConvert(TypeDescription.createVarchar(255), binaryCol, 0, "hello", payload);

        BinaryColumnVector emptyBinaryCol = new BinaryColumnVector(1);
        emptyBinaryCol.add(new byte[0]);
        assertConvert(TypeDescription.createVarchar(255), emptyBinaryCol, 0, "", new byte[0]);
    }

    @Test
    public void testConvertNullAndEmptySql()
    {
        ByteColumnVector col = new ByteColumnVector(1);
        col.vector[0] = 42;
        col.isNull[0] = true;
        assertNull(TypeDescription.createByte().convertColumnVectorToByte(col, 0));

        assertNull(TypeDescription.createVarchar(16).convertSqlStringToByte(null));
        assertArrayEquals(new byte[0], TypeDescription.createVarchar(16).convertSqlStringToByte(""));
        assertArrayEquals("  value  ".getBytes(StandardCharsets.UTF_8),
                TypeDescription.createVarchar(16).convertSqlStringToByte("  value  "));

        assertNull(TypeDescription.createInt().convertSqlStringToByte(null));
        assertNull(TypeDescription.createInt().convertSqlStringToByte(""));
        assertSqlConversionFails(TypeDescription.createInt(), "   ");
    }

    @Test
    public void testConvertBooleanAndByteStrictly()
    {
        TypeDescription booleanType = TypeDescription.createBoolean();
        assertArrayEquals(new byte[]{0}, booleanType.convertSqlStringToByte("0"));
        assertArrayEquals(new byte[]{1}, booleanType.convertSqlStringToByte("1"));
        assertArrayEquals(new byte[]{0}, booleanType.convertSqlStringToByte("FaLsE"));
        assertArrayEquals(new byte[]{1}, booleanType.convertSqlStringToByte("TrUe"));
        assertSqlConversionFails(booleanType, "2");
        assertSqlConversionFails(booleanType, "yes");
        assertSqlConversionFails(booleanType, "10");

        TypeDescription byteType = TypeDescription.createByte();
        assertArrayEquals(new byte[]{Byte.MIN_VALUE},
                byteType.convertSqlStringToByte(Byte.toString(Byte.MIN_VALUE)));
        assertArrayEquals(new byte[]{Byte.MAX_VALUE},
                byteType.convertSqlStringToByte(Byte.toString(Byte.MAX_VALUE)));
        assertSqlConversionFails(byteType, "-129");
        assertSqlConversionFails(byteType, "128");
    }

    private static TypeDescription parseBoth(String typeExpression)
    {
        TypeDescription fromString = TypeDescription.fromString(typeExpression);
        TypeDescription fromSchema = TypeDescription.createSchemaFromStrings(
                Collections.singletonList("column"),
                Collections.singletonList(typeExpression)).getChildren().get(0);
        assertEquivalent(fromString, fromSchema);
        return fromString;
    }

    private static void assertIllegal(String typeExpression)
    {
        try
        {
            TypeDescription.fromString(typeExpression);
            fail("Expected invalid type expression: " + typeExpression);
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }

        try
        {
            TypeDescription.createSchemaFromStrings(
                    Collections.singletonList("column"),
                    Collections.singletonList(typeExpression));
            fail("Expected invalid type expression: " + typeExpression);
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }

    private static void assertSqlConversionFails(TypeDescription type, String sql)
    {
        try
        {
            type.convertSqlStringToByte(sql);
            fail("Expected SQL conversion failure for type " + type + " value: " + sql);
        }
        catch (IllegalArgumentException expected)
        {
            // expected, NumberFormatException from numeric parsers is a subclass of it
        }
    }

    private static void assertConvert(TypeDescription type, ColumnVector col, int row,
                                      String sqlValue, byte[] expected)
    {
        byte[] fromVector = type.convertColumnVectorToByte(col, row);
        byte[] fromSql = type.convertSqlStringToByte(sqlValue);
        assertArrayEquals(expected, fromVector);
        assertArrayEquals(expected, fromSql);
    }

    private static void assertEquivalent(TypeDescription expected, TypeDescription actual)
    {
        assertEquals(expected.getCategory(), actual.getCategory());
        assertEquals(expected.getMaxLength(), actual.getMaxLength());
        assertEquals(expected.getPrecision(), actual.getPrecision());
        assertEquals(expected.getScale(), actual.getScale());
        assertEquals(expected.getDimension(), actual.getDimension());

        List<TypeDescription> expectedChildren = expected.getChildren();
        List<TypeDescription> actualChildren = actual.getChildren();
        if (expectedChildren == null)
        {
            assertNull(actualChildren);
            return;
        }

        assertNotNull(actualChildren);
        assertEquals(expectedChildren.size(), actualChildren.size());
        if (expected.getCategory() == TypeDescription.Category.STRUCT)
        {
            assertEquals(expected.getFieldNames(), actual.getFieldNames());
        }
        for (int i = 0; i < expectedChildren.size(); i++)
        {
            assertEquivalent(expectedChildren.get(i), actualChildren.get(i));
        }
    }
}
