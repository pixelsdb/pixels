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
package io.pixelsdb.pixels.common.ingest.wire;

import java.io.*;
import java.util.*;

/** Bounded column-major scalar batches. A length of -1 encodes SQL NULL. */
public final class ColumnBatchCodec {
    public static final int FORMAT = 1;
    private static final int MAGIC = 0x50494231;
    private static final int HEADER_BYTES = 3 * Integer.BYTES; // magic, rows, columns

    private ColumnBatchCodec() {}

    public static byte[] encode(List<byte[][]> rows, int columns, int maxBytes) throws IOException {
        long size = HEADER_BYTES + (long) Integer.BYTES * columns * rows.size();
        for (byte[][] row : rows) {
            if (row.length != columns) throw new IOException("Column count mismatch");
            for (byte[] v : row) if (v != null) size += v.length;
        }
        if (columns <= 0 || rows.isEmpty() || size > maxBytes)
            throw new IOException("Invalid or oversized column batch");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream((int) size);
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(MAGIC);
        out.writeInt(rows.size());
        out.writeInt(columns);
        for (int column = 0; column < columns; column++)
            for (byte[][] row : rows) {
                byte[] v = row[column];
                out.writeInt(v == null ? -1 : v.length);
                if (v != null) out.write(v);
            }
        out.flush();
        return bytes.toByteArray();
    }

    /** Visits cells in column order without allocating row arrays or copying scalar bytes.
     * The payload belongs to the caller and must not be modified by the visitor.
     * A length of -1 denotes NULL; zero denotes an empty value.
     */
    @FunctionalInterface
    public interface CellVisitor {
        void visit(int column, int row, byte[] payload, int offset, int length) throws IOException;
    }

    public static void visit(
            byte[] payload, int expectedRows, int expectedColumns, int maxRows, int maxBytes,
            CellVisitor visitor) throws IOException {
        java.nio.ByteBuffer input = header(payload, expectedRows, expectedColumns, maxRows, maxBytes);
        cells(input, payload, expectedRows, expectedColumns, visitor);
    }

    /** A forward-only, column-major reader with one position per column, not per row. */
    public static final class Cursor {
        private final byte[] payload;
        private final java.nio.ByteBuffer input;
        private final int[] positions;
        private final int rows;
        private int consumed;

        private Cursor(byte[] payload, int rows, int columns, int maxRows, int maxBytes,
                       CellVisitor validator) throws IOException {
            this.payload = payload;
            this.rows = rows;
            this.positions = new int[columns];
            this.input = header(payload, rows, columns, maxRows, maxBytes);
            cells(input, payload, rows, columns, (column, row, bytes, offset, length) -> {
                if (row == 0) positions[column] = offset - Integer.BYTES;
                validator.visit(column, row, bytes, offset, length);
            });
        }

        public int remaining() {
            return rows - consumed;
        }

        /** Row indexes passed to the visitor are relative to this window. */
        public void next(int count, CellVisitor visitor) throws IOException {
            if (count < 0 || count > remaining()) throw new IOException("Invalid batch window");
            for (int column = 0; column < positions.length; column++) {
                input.position(positions[column]);
                for (int row = 0; row < count; row++) {
                    int length = input.getInt();
                    int offset = input.position();
                    visitor.visit(column, row, payload, offset, length);
                    if (length >= 0) input.position(offset + length);
                }
                positions[column] = input.position();
            }
            consumed += count;
        }

        public void skip(int count) throws IOException {
            next(count, (column, row, bytes, offset, length) -> {});
        }
    }

    /** The caller must retain the immutable payload until the cursor is exhausted. */
    public static Cursor cursor(byte[] payload, int rows, int columns, int maxRows, int maxBytes,
                                CellVisitor validator) throws IOException {
        return new Cursor(payload, rows, columns, maxRows, maxBytes, validator);
    }

    public static List<byte[][]> decode(
            byte[] payload, int expectedRows, int expectedColumns, int maxRows, int maxBytes)
            throws IOException {
        java.nio.ByteBuffer input = header(payload, expectedRows, expectedColumns, maxRows, maxBytes);
        List<byte[][]> rows = new ArrayList<>(expectedRows);
        for (int i = 0; i < expectedRows; i++) rows.add(new byte[expectedColumns][]);
        cells(input, payload, expectedRows, expectedColumns, (column, row, source, offset, length) -> {
            if (length >= 0) rows.get(row)[column] = Arrays.copyOfRange(source, offset, offset + length);
        });
        return rows;
    }

    private static java.nio.ByteBuffer header(
            byte[] payload, int expectedRows, int expectedColumns, int maxRows, int maxBytes)
            throws IOException {
        if (payload.length > maxBytes
                || expectedRows <= 0
                || expectedRows > maxRows
                || expectedColumns <= 0) throw new IOException("Batch limits exceeded");
        if (payload.length < HEADER_BYTES) throw new IOException("Truncated batch header");
        java.nio.ByteBuffer input = java.nio.ByteBuffer.wrap(payload);
        if (input.getInt() != MAGIC
                || input.getInt() != expectedRows
                || input.getInt() != expectedColumns)
            throw new IOException("Batch metadata mismatch");
        if ((long) Integer.BYTES * expectedRows * expectedColumns > input.remaining())
            throw new IOException("Truncated batch");
        return input;
    }

    private static void cells(
            java.nio.ByteBuffer input, byte[] payload, int rows, int columns, CellVisitor visitor)
            throws IOException {
        for (int column = 0; column < columns; column++) {
            for (int row = 0; row < rows; row++) {
                if (input.remaining() < Integer.BYTES) throw new IOException("Truncated cell length");
                int length = input.getInt();
                if (length < -1 || length > input.remaining())
                    throw new IOException("Invalid cell length");
                int offset = input.position();
                visitor.visit(column, row, payload, offset, length);
                if (length >= 0) input.position(offset + length);
            }
        }
        if (input.hasRemaining()) throw new IOException("Trailing batch bytes");
    }
}
