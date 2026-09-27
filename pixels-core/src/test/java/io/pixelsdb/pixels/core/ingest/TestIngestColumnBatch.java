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

import io.pixelsdb.pixels.common.ingest.wire.ColumnBatchCodec;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.vector.BinaryColumnVector;
import io.pixelsdb.pixels.core.vector.ColumnVector;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.ingest.IngestProto.TableColumn;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;

public class TestIngestColumnBatch {
    private static final int MAX_BYTES = 4096;
    private static final int WINDOW_ROWS = 2;
    private static final long COMMIT_TIMESTAMP = 37;
    private static final String[] TYPES = {"boolean", "tinyint", "smallint", "int", "bigint",
            "float", "double", "date", "time(3)", "timestamp(6)", "decimal(18,2)",
            "decimal(38,2)", "char(8)", "varchar(8)", "string", "binary", "varbinary(8)"};

    @Test
    public void windowsMatchRowPathWithPhysicalReordering() throws Exception {
        TableSpec.Builder table = TableSpec.newBuilder();
        TypeDescription schema = TypeDescription.createStruct();
        int[] outputColumns = new int[TYPES.length];
        byte[][] values = new byte[TYPES.length][];
        for (int column = 0; column < TYPES.length; column++) {
            table.addColumns(TableColumn.newBuilder().setName("c" + column).setType(TYPES[column]));
            outputColumns[column] = TYPES.length - column - 1;
            schema.addField("c" + column, IngestRows.supported(TYPES[outputColumns[column]]));
            values[column] = value(IngestRows.supported(TYPES[column]));
        }
        List<byte[][]> rows = Arrays.asList(values, new byte[TYPES.length][], values, values);
        byte[] payload = ColumnBatchCodec.encode(rows, TYPES.length, MAX_BYTES);
        IngestColumnBatch source = new IngestColumnBatch(table.build(), payload, rows.size(),
                rows.size(), MAX_BYTES);
        VectorizedRowBatch actual = schema.createRowBatchWithHiddenColumn(WINDOW_ROWS);
        VectorizedRowBatch expected = schema.createRowBatchWithHiddenColumn(WINDOW_ROWS);
        source.appendWindow(actual, outputColumns, WINDOW_ROWS, COMMIT_TIMESTAMP);
        compare(rows.subList(0, WINDOW_ROWS), actual, expected, outputColumns);
        for (ColumnVector vector : actual.cols) {
            if (vector instanceof BinaryColumnVector)
                assertSame(payload, ((BinaryColumnVector) vector).vector[0]);
        }
        actual.reset();
        expected.reset();
        source.skip(1);
        source.appendWindow(actual, outputColumns, 1, COMMIT_TIMESTAMP);
        compare(rows.subList(rows.size() - 1, rows.size()), actual, expected, outputColumns);
        assertEquals(0, source.remaining());
        assertThrows(IOException.class, () -> source.skip(1));
        actual.close();
        expected.close();
    }

    @Test
    public void emptyStringIsNotNull() throws Exception {
        TableSpec table = TableSpec.newBuilder().addColumns(TableColumn.newBuilder()
                .setName("s").setType("string")).build();
        List<byte[][]> rows = Arrays.asList(new byte[][] {new byte[0]}, new byte[][] {null});
        byte[] payload = ColumnBatchCodec.encode(rows, 1, MAX_BYTES);
        IngestColumnBatch source = new IngestColumnBatch(table, payload, rows.size(), rows.size(), MAX_BYTES);
        VectorizedRowBatch batch = TypeDescription.createStruct().addField("s",
                TypeDescription.createString()).createRowBatchWithHiddenColumn(WINDOW_ROWS);
        source.appendWindow(batch, new int[] {0}, WINDOW_ROWS, COMMIT_TIMESTAMP);
        assertFalse(batch.cols[0].isNull[0]);
        assertTrue(batch.cols[0].isNull[1]);
        assertEquals(0, ((BinaryColumnVector) batch.cols[0]).lens[0]);
        batch.close();
    }

    private void compare(List<byte[][]> rows, VectorizedRowBatch actual, VectorizedRowBatch expected,
                         int[] outputColumns) {
        for (byte[][] row : rows) {
            for (int column = 0; column < row.length; column++) {
                if (row[column] == null) expected.cols[outputColumns[column]].addNull();
                else expected.cols[outputColumns[column]].add(row[column]);
            }
            expected.cols[TYPES.length].add(COMMIT_TIMESTAMP);
            expected.size++;
        }
        assertEquals(expected.size, actual.size);
        for (int column = 0; column < actual.cols.length; column++) {
            for (int row = 0; row < expected.size; row++) {
                assertEquals(expected.cols[column].isNull[row], actual.cols[column].isNull[row]);
                if (!expected.cols[column].isNull[row])
                    assertTrue("value at column " + column,
                            expected.cols[column].elementEquals(row, row, actual.cols[column]));
            }
        }
    }

    private byte[] value(TypeDescription type) {
        switch (type.getCategory()) {
            case BOOLEAN:
            case BYTE: return new byte[] {1};
            case SHORT: return ByteBuffer.allocate(Short.BYTES).putShort((short) -2).array();
            case INT:
            case DATE:
            case TIME: return ByteBuffer.allocate(Integer.BYTES).putInt(3).array();
            case FLOAT: return ByteBuffer.allocate(Float.BYTES).putFloat(-1.5f).array();
            case DOUBLE: return ByteBuffer.allocate(Double.BYTES).putDouble(-2.5).array();
            case LONG:
            case TIMESTAMP: return ByteBuffer.allocate(Long.BYTES).putLong(4).array();
            case DECIMAL:
                if (type.getPrecision() <= TypeDescription.MAX_SHORT_DECIMAL_PRECISION)
                    return ByteBuffer.allocate(Long.BYTES).putLong(-5).array();
                return ByteBuffer.allocate(2 * Long.BYTES).putLong(-1).putLong(-5).array();
            default: return new byte[] {'a', 'b'};
        }
    }
}
