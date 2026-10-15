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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.core.ingest;

import io.pixelsdb.pixels.ingest.IngestProto.TableColumn;
import io.pixelsdb.pixels.ingest.IngestProto.TableIndex;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

import static org.junit.Assert.*;

public class TestIngestRows {
    private static final int SLICE_OFFSET = 3;

    private TableSpec table(String type) {
        return TableSpec.newBuilder().addColumns(TableColumn.newBuilder()
                .setName("value").setType(type)).build();
    }

    @Test
    public void scalarSlicesMatchRows() throws Exception {
        String[] types = {"boolean", "tinyint", "smallint", "int", "bigint", "float",
                "double", "date", "time(3)", "timestamp(6)"};
        int[] widths = {Byte.BYTES, Byte.BYTES, Short.BYTES, Integer.BYTES, Long.BYTES,
                Integer.BYTES, Long.BYTES, Integer.BYTES, Integer.BYTES, Long.BYTES};
        for (int i = 0; i < types.length; i++) {
            IngestRows.Validator validator = new IngestRows.Validator(table(types[i]));
            byte[] value = new byte[widths[i]];
            validator.validate(new byte[][] {value});
            validator.validateCell(0, slice(value), SLICE_OFFSET, value.length);
            byte[] invalid = new byte[value.length + 1];
            assertThrows(IOException.class, () -> validator.validate(new byte[][] {invalid}));
            assertThrows(IOException.class, () -> validator.validateCell(0, slice(invalid),
                    SLICE_OFFSET, invalid.length));
        }
        IngestRows.Validator bool = new IngestRows.Validator(table("boolean"));
        assertThrows(IOException.class, () -> bool.validateCell(0, new byte[] {2}, 0, Byte.BYTES));
    }

    @Test
    public void preservesNullAndEmptyValuesAndIndexConstraints() throws Exception {
        IngestRows.Validator validator = new IngestRows.Validator(table("string"));
        validator.validate(new byte[][] {null});
        validator.validateCell(0, null, 0, -1);
        validator.validateCell(0, new byte[0], 0, 0);
        TableIndex index = TableIndex.newBuilder().addColumns(0).build();
        IngestRows.Validator indexed = new IngestRows.Validator(table("string").toBuilder()
                .addIndexes(index).build());
        assertThrows(IOException.class, () -> indexed.validateCell(0, null, 0, -1));
        indexed.validateCell(0, new byte[0], 0, 0);
        assertThrows(IOException.class, () -> new IngestRows.Validator(table("string").toBuilder()
                .addIndexes(index.toBuilder().setUnique(true)).build()));
        assertThrows(IOException.class, () -> new IngestRows.Validator(table("string").toBuilder()
                .addIndexes(TableIndex.newBuilder().addColumns(1)).build()));
        assertThrows(IOException.class, () -> validator.validateCell(1, null, 0, -1));
        assertThrows(IOException.class, () -> validator.validateCell(0, new byte[0], 1, 0));
    }

    @Test
    public void decimalSlicesPreserveSignedPrecisionChecks() throws Exception {
        for (int precision : new int[] {18, 38}) {
            IngestRows.Validator validator = new IngestRows.Validator(table("decimal(" + precision + ",0)"));
            int width = precision <= 18 ? Long.BYTES : 2 * Long.BYTES;
            BigInteger overflow = BigInteger.TEN.pow(precision);
            for (int sign : new int[] {-1, 1}) {
                byte[] valid = decimal(overflow.subtract(BigInteger.ONE)
                        .multiply(BigInteger.valueOf(sign)), width);
                validator.validate(new byte[][] {valid});
                validator.validateCell(0, slice(valid), SLICE_OFFSET, width);
                byte[] invalid = decimal(overflow.multiply(BigInteger.valueOf(sign)), width);
                assertThrows(IOException.class, () -> validator.validate(new byte[][] {invalid}));
                assertThrows(IOException.class, () -> validator.validateCell(0, slice(invalid),
                        SLICE_OFFSET, width));
            }
        }
    }

    private byte[] slice(byte[] value) {
        byte[] payload = new byte[SLICE_OFFSET + value.length];
        System.arraycopy(value, 0, payload, SLICE_OFFSET, value.length);
        return payload;
    }

    private byte[] decimal(BigInteger value, int width) {
        byte[] result = new byte[width];
        if (value.signum() < 0) Arrays.fill(result, (byte) -1);
        byte[] encoded = value.toByteArray();
        System.arraycopy(encoded, 0, result, width - encoded.length, encoded.length);
        return result;
    }
}
