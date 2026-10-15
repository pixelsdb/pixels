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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TestColumnBatchCodec {
    private static final int MAX_ROWS = 16;
    private static final int MAX_BYTES = 1024;
    private static final int COLUMNS = 2;
    private static final int HEADER_BYTES = 3 * Integer.BYTES;

    private List<byte[][]> rows() {
        return Arrays.asList(new byte[][] {null, {1, 2}}, new byte[][] {{}, {3}});
    }

    @Test
    void visitsOriginalPayloadInColumnOrder() throws Exception {
        List<byte[][]> expected = rows();
        byte[] payload = ColumnBatchCodec.encode(expected, COLUMNS, MAX_BYTES);
        int[] visited = {0};
        ColumnBatchCodec.visit(payload, expected.size(), COLUMNS, MAX_ROWS, MAX_BYTES,
                (column, row, source, offset, length) -> {
                    assertSame(payload, source);
                    assertEquals(visited[0] / expected.size(), column);
                    assertEquals(visited[0] % expected.size(), row);
                    byte[] value = expected.get(row)[column];
                    if (value == null) assertEquals(-1, length);
                    else assertArrayEquals(value, Arrays.copyOfRange(source, offset, offset + length));
                    visited[0]++;
                });
        assertEquals(expected.size() * COLUMNS, visited[0]);
        List<byte[][]> decoded = ColumnBatchCodec.decode(payload, expected.size(), COLUMNS,
                MAX_ROWS, MAX_BYTES);
        Arrays.fill(payload, (byte) 0);
        for (int row = 0; row < expected.size(); row++)
            for (int column = 0; column < COLUMNS; column++)
                assertArrayEquals(expected.get(row)[column], decoded.get(row)[column]);
    }

    @Test
    void bothPathsRejectMalformedBatches() throws Exception {
        byte[] payload = ColumnBatchCodec.encode(rows(), COLUMNS, MAX_BYTES);
        for (int length = 0; length < payload.length; length++)
            reject(Arrays.copyOf(payload, length));
        reject(Arrays.copyOf(payload, payload.length + 1));
        for (int offset = 0; offset < HEADER_BYTES; offset += Integer.BYTES) {
            byte[] invalid = payload.clone();
            ByteBuffer.wrap(invalid).putInt(offset, 0);
            reject(invalid);
        }
        for (int invalidLength : new int[] {-2, Integer.MAX_VALUE}) {
            byte[] invalid = payload.clone();
            ByteBuffer.wrap(invalid).putInt(HEADER_BYTES, invalidLength);
            reject(invalid);
        }
        assertThrows(IOException.class, () -> ColumnBatchCodec.visit(payload, rows().size(),
                COLUMNS, rows().size() - 1, MAX_BYTES, (c, r, p, o, n) -> {}));
        assertThrows(IOException.class, () -> ColumnBatchCodec.decode(payload, rows().size(),
                COLUMNS, MAX_ROWS, payload.length - 1));
    }

    @Test
    void cursorPreservesWindowsAndRejectsOverrun() throws Exception {
        List<byte[][]> expected = rows();
        byte[] payload = ColumnBatchCodec.encode(expected, COLUMNS, MAX_BYTES);
        ColumnBatchCodec.Cursor cursor = ColumnBatchCodec.cursor(payload, expected.size(), COLUMNS,
                MAX_ROWS, MAX_BYTES, (c, r, p, o, n) -> {});
        assertThrows(IOException.class, () -> cursor.skip(expected.size() + 1));
        assertEquals(expected.size(), cursor.remaining());
        cursor.skip(1);
        cursor.next(1, (column, row, bytes, offset, length) -> {
            assertEquals(0, row);
            assertSame(payload, bytes);
            assertArrayEquals(expected.get(1)[column], Arrays.copyOfRange(bytes, offset, offset + length));
        });
        assertEquals(0, cursor.remaining());
        assertThrows(IOException.class, () -> cursor.skip(1));
    }

    @Test
    void propagatesValidationFailure() throws Exception {
        byte[] payload = ColumnBatchCodec.encode(rows(), COLUMNS, MAX_BYTES);
        IOException failure = new IOException("Invalid scalar");
        assertSame(failure, assertThrows(IOException.class, () -> ColumnBatchCodec.visit(payload,
                rows().size(), COLUMNS, MAX_ROWS, MAX_BYTES, (c, r, p, o, n) -> {throw failure;})));
    }

    private void reject(byte[] payload) {
        assertThrows(IOException.class, () -> ColumnBatchCodec.cursor(payload, rows().size(),
                COLUMNS, MAX_ROWS, MAX_BYTES, (c, r, p, o, n) -> {}));
        assertThrows(IOException.class, () -> ColumnBatchCodec.decode(payload, rows().size(),
                COLUMNS, MAX_ROWS, MAX_BYTES));
        assertThrows(IOException.class, () -> ColumnBatchCodec.visit(payload, rows().size(),
                COLUMNS, MAX_ROWS, MAX_BYTES, (c, r, p, o, n) -> {}));
    }
}
