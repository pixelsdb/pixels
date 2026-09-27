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

import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.*;
import io.pixelsdb.pixels.common.node.NodeService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.daemon.NodeProto;
import io.pixelsdb.pixels.ingest.IngestProto.*;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;

/** Pins metadata and existing bucket ownership for deterministic preparation and replay. */
public final class IngestTables {
    private IngestTables() {}

    public static List<Route> routes() throws Exception {
        int count = Integer.parseInt(ConfigFactory.Instance().getProperty("node.bucket.num"));
        List<Route> out = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            NodeProto.NodeInfo n = NodeService.Instance().getRetinaByBucket(i);
            if (n == null || n.getAddress().isEmpty() || n.getPort() == 0)
                throw new IOException("Missing Retina route");
            out.add(
                    Route.newBuilder()
                            .setShardId(i)
                            .setHost(n.getAddress())
                            .setPort(n.getPort())
                            .setVirtualNodeId(n.getVirtualNodeId())
                            .build());
        }
        return out;
    }

    public static TableSpec load(String schema, String name) throws Exception {
        MetadataService m = MetadataService.Instance();
        Table table = m.getTable(schema, name);
        Layout layout = m.getLatestLayout(schema, name);
        if (table == null || layout == null)
            throw new IOException("Table or writable layout missing");
        List<Column> columns = m.getColumns(schema, name, false);
        TableSpec.Builder b =
                TableSpec.newBuilder()
                        .setTableId(table.getId())
                        .setSchemaName(schema)
                        .setTableName(name)
                        .setSchemaVersion(layout.getSchemaVersionId())
                        .setLayoutId(layout.getId())
                        .addAllRoutes(routes());
        Map<Long, Integer> ord = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            IngestRows.supported(column.getType());
            ord.put(column.getId(), i);
            b.addColumns(
                    TableColumn.newBuilder()
                            .setId(column.getId())
                            .setName(column.getName())
                            .setType(column.getType()));
        }
        List<SinglePointIndex> indexes = m.getSinglePointIndices(table.getId());
        if (indexes == null) {
            indexes = Collections.emptyList();
        }
        for (SinglePointIndex index : indexes) {
            TableIndex.Builder x =
                    TableIndex.newBuilder()
                            .setId(index.getId())
                            .setPrimary(index.isPrimary())
                            .setUnique(index.isUnique());
            for (int id : index.getKeyColumns().getKeyColumnIds()) {
                Integer i = ord.get((long) id);
                if (i == null) throw new IOException("Index schema differs from table");
                x.addColumns(i);
            }
            b.addIndexes(x);
        }
        TableSpec plain = b.build();
        return b.setFingerprint(
                        ByteString.copyFrom(
                                MessageDigest.getInstance("SHA-256").digest(plain.toByteArray())))
                .build();
    }

    public static void validate(TableSpec expected) throws Exception {
        if (!load(expected.getSchemaName(), expected.getTableName()).equals(expected))
            throw new IOException("Schema, layout, index, or route changed during write");
    }
}
