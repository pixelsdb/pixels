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

    /** Resolves the pinned column types once for a batch of native encoded rows. */
    public static final class Validator {
        private final TableSpec table;
        private final TypeDescription[] types;

        public Validator(TableSpec table) throws IOException {
            this.table = table;
            this.types = new TypeDescription[table.getColumnsCount()];
            for (int i = 0; i < types.length; i++) {
                types[i] = supported(table.getColumns(i).getType());
            }
        }

        public void validate(byte[][] row) throws IOException {
            validateRow(table, types, row);
        }
    }

    private static void validateRow(TableSpec table, TypeDescription[] types, byte[][] row)
            throws IOException {
        if (row.length != table.getColumnsCount())
            throw new IOException("Column count differs from pinned schema");
        for (int i = 0; i < row.length; i++) {
            TypeDescription t = types[i];
            byte[] b = row[i];
            if (b == null) continue;
            int width = -1;
            switch (t.getCategory()) {
                case BOOLEAN:
                    width = 1;
                    if (b.length == 1 && b[0] != 0 && b[0] != 1)
                        throw new IOException("Invalid BOOLEAN");
                    break;
                case BYTE:
                    width = 1;
                    break;
                case SHORT:
                    width = 2;
                    break;
                case INT:
                case DATE:
                case FLOAT:
                case TIME:
                    width = 4;
                    break;
                case LONG:
                case TIMESTAMP:
                case DOUBLE:
                    width = 8;
                    break;
                case DECIMAL:
                    width = t.getPrecision() <= 18 ? 8 : 16;
                    if (b.length == width
                            && new BigInteger(b).abs().toString().length() > t.getPrecision())
                        throw new IOException("DECIMAL precision overflow");
                    break;
                default:
                    break;
            }
            if (width != -1 && b.length != width)
                throw new IOException("Invalid scalar byte width for " + t);
        }
        for (TableIndex index : table.getIndexesList()) {
            if (index.getUnique() && !index.getPrimary())
                throw new IOException("Unique secondary constraints are not enabled");
            for (int column : index.getColumnsList())
                if (column < 0 || column >= row.length || row[column] == null)
                    throw new IOException("NULL or invalid indexed column is unsupported");
        }
    }
}
