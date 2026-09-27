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
import io.pixelsdb.pixels.core.vector.*;
import io.pixelsdb.pixels.ingest.IngestProto.TableSpec;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Validated columnar input retained only until its bounded write windows have been consumed. */
public final class IngestColumnBatch {
    private final ByteBuffer payload;
    private final ColumnBatchCodec.Cursor cursor;
    private final TypeDescription[] types;

    public IngestColumnBatch(TableSpec table, byte[] payload, int rows, int maxRows, int maxBytes)
            throws IOException {
        this.payload = ByteBuffer.wrap(payload);
        this.types = new TypeDescription[table.getColumnsCount()];
        for (int column = 0; column < types.length; column++)
            types[column] = IngestRows.supported(table.getColumns(column).getType());
        IngestRows.Validator validator = new IngestRows.Validator(table);
        this.cursor = ColumnBatchCodec.cursor(payload, rows, types.length, maxRows, maxBytes,
                (column, row, bytes, offset, length) ->
                        validator.validateCell(column, bytes, offset, length));
    }

    public int remaining() {
        return cursor.remaining();
    }

    public void skip(int count) throws IOException {
        cursor.skip(count);
    }

    /** Source-to-output mapping is the inverse of the writer's physical column order. */
    public void appendWindow(VectorizedRowBatch batch, int[] outputColumns, int count,
                             long commitTimestamp) throws IOException {
        if (batch.size != 0 || count < 0 || count > batch.getMaxSize()
                || outputColumns.length != types.length || batch.cols.length != types.length + 1)
            throw new IOException("Invalid columnar write window");
        cursor.next(count, (column, row, bytes, offset, length) -> {
            ColumnVector vector = batch.cols[outputColumns[column]];
            if (length == -1) {
                vector.addNull();
                return;
            }
            switch (types[column].getCategory()) {
                case BOOLEAN:
                    vector.add(payload.get(offset) != 0);
                    break;
                case BYTE:
                    vector.add(payload.get(offset));
                    break;
                case SHORT:
                    ((ShortColumnVector) vector).add(payload.getShort(offset));
                    break;
                case INT:
                case DATE:
                case TIME:
                case FLOAT:
                    vector.add(payload.getInt(offset));
                    break;
                case LONG:
                case TIMESTAMP:
                    vector.add(payload.getLong(offset));
                    break;
                case DOUBLE:
                    // Preserve the native bit representation, including NaN payloads.
                    ((DoubleColumnVector) vector).vector[row] = payload.getLong(offset);
                    finishScalar(vector, row);
                    break;
                case DECIMAL:
                    if (types[column].getPrecision() <= TypeDescription.MAX_SHORT_DECIMAL_PRECISION) {
                        ((DecimalColumnVector) vector).vector[row] = payload.getLong(offset);
                    } else {
                        long[] values = ((LongDecimalColumnVector) vector).vector;
                        values[row * 2] = payload.getLong(offset);
                        values[row * 2 + 1] = payload.getLong(offset + Long.BYTES);
                    }
                    finishScalar(vector, row);
                    break;
                case CHAR:
                case VARCHAR:
                case STRING:
                case BINARY:
                case VARBINARY:
                    ((BinaryColumnVector) vector).setRef(row, bytes, offset, length);
                    break;
                default:
                    throw new IOException("Unsupported columnar ingestion type: " + types[column]);
            }
        });
        for (int row = 0; row < count; row++) batch.cols[types.length].add(commitTimestamp);
        batch.size = count;
    }

    private static void finishScalar(ColumnVector vector, int row) {
        vector.isNull[row] = false;
        vector.setWriteIndex(row + 1);
    }
}
