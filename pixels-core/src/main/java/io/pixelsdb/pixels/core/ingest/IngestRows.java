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

import com.google.protobuf.ByteString;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.utils.PrimaryKeyBytes;
import io.pixelsdb.pixels.ingest.IngestProto.*;

import java.io.IOException;
import java.math.BigInteger;

/** Validates native scalar encodings; existing index keys retain their encoding. */
public final class IngestRows {
    private IngestRows() {}

    public static TypeDescription supported(String name) throws IOException {
        TypeDescription t;
        try {
            t = TypeDescription.fromString(name);
        } catch (RuntimeException e) {
            throw new IOException("Invalid storage type: " + name, e);
        }
        switch (t.getCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DATE:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case STRING:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
                return t;
            case TIME:
                if (t.getPrecision() > 3)
                    throw new IOException("TIME precision exceeds milliseconds");
                return t;
            case TIMESTAMP:
                if (t.getPrecision() > 6)
                    throw new IOException("TIMESTAMP precision exceeds microseconds");
                return t;
            default:
                throw new IOException("Unsupported INSERT storage type: " + name);
        }
    }

    public static TableIndex primary(TableSpec table) {
        TableIndex found = null;
        for (TableIndex i : table.getIndexesList())
            if (i.getPrimary()) {
                if (found != null) throw new IllegalArgumentException("Multiple primary indexes");
                found = i;
            }
        return found;
    }

    public static ByteString indexKey(TableIndex index, byte[][] row) {
        byte[][] parts = new byte[index.getColumnsCount()][];
        for (int i = 0; i < parts.length; i++) parts[i] = row[index.getColumns(i)];
        return ByteString.copyFrom(PrimaryKeyBytes.concat(parts));
    }

    public static void validate(TableSpec table, byte[][] row) throws IOException {
        new Validator(table).validate(row);
    }

    /** Resolves pinned column types and index nullability once per batch. */
    public static final class Validator {
        private final TypeDescription[] types;
        private final boolean[] indexed;

        public Validator(TableSpec table) throws IOException {
            this.types = new TypeDescription[table.getColumnsCount()];
            this.indexed = new boolean[types.length];
            for (int i = 0; i < types.length; i++) {
                types[i] = supported(table.getColumns(i).getType());
            }
            for (TableIndex index : table.getIndexesList()) {
                if (index.getUnique() && !index.getPrimary())
                    throw new IOException("Unique secondary constraints are not enabled");
                for (int column : index.getColumnsList()) {
                    if (column < 0 || column >= types.length)
                        throw new IOException("Invalid indexed column");
                    indexed[column] = true;
                }
            }
        }

        public void validate(byte[][] row) throws IOException {
            if (row.length != types.length)
                throw new IOException("Column count differs from pinned schema");
            for (int column = 0; column < row.length; column++) {
                byte[] value = row[column];
                validateCell(column, value, 0, value == null ? -1 : value.length);
            }
        }

        /** Validates a scalar directly in its encoded batch; -1 length denotes SQL NULL. */
        public void validateCell(int column, byte[] payload, int offset, int length) throws IOException {
            if (column < 0 || column >= types.length)
                throw new IOException("Column outside pinned schema");
            if (length == -1) {
                if (indexed[column]) throw new IOException("NULL indexed column is unsupported");
                return;
            }
            if (payload == null || offset < 0 || length < 0 || offset > payload.length - length)
                throw new IOException("Invalid scalar range");
            validateScalar(types[column], payload, offset, length);
        }
    }

    private static void validateScalar(TypeDescription type, byte[] bytes, int offset, int length)
            throws IOException {
        int width = -1;
        switch (type.getCategory()) {
            case BOOLEAN:
                width = Byte.BYTES;
                if (length == width && bytes[offset] != 0 && bytes[offset] != 1)
                    throw new IOException("Invalid BOOLEAN");
                break;
            case BYTE:
                width = Byte.BYTES;
                break;
            case SHORT:
                width = Short.BYTES;
                break;
            case INT:
            case DATE:
            case FLOAT:
            case TIME:
                width = Integer.BYTES;
                break;
            case LONG:
            case TIMESTAMP:
            case DOUBLE:
                width = Long.BYTES;
                break;
            case DECIMAL:
                width = type.getPrecision() <= TypeDescription.MAX_SHORT_DECIMAL_PRECISION
                        ? Long.BYTES : Long.BYTES * 2;
                if (length == width) {
                    byte[] value = offset == 0 && length == bytes.length ? bytes
                            : java.util.Arrays.copyOfRange(bytes, offset, offset + length);
                    if (new BigInteger(value).abs().toString().length() > type.getPrecision())
                        throw new IOException("DECIMAL precision overflow");
                }
                break;
            default:
                break;
        }
        if (width != -1 && length != width)
            throw new IOException("Invalid scalar byte width for " + type);
    }
}
