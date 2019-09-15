/*
 * Copyright 2018 PixelsDB.
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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author: tao
 * @date: Create in 2018-01-19 15:16
 **/
public class PixelsTable {
    private final PixelsTableHandle table;
    private final PixelsTableLayoutHandle tableLayout;
    private final List<PixelsColumnHandle> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public PixelsTable(
            @JsonProperty("table") PixelsTableHandle table,
            @JsonProperty("tableLayout") PixelsTableLayoutHandle tableLayout,
            @JsonProperty("columns") List<PixelsColumnHandle> columns,
            @JsonProperty("columnsMetadata") List<ColumnMetadata> columnsMetadata) {
        this.table = requireNonNull(table, "table is null");
        this.tableLayout = requireNonNull(tableLayout, "name is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.columnsMetadata = requireNonNull(columnsMetadata, "columnsMetadata is null");
    }

    @JsonProperty
    public PixelsTableHandle getTableHandle() {
        return table;
    }

    @JsonProperty
    public PixelsTableLayoutHandle getTableLayout() {
        return tableLayout;
    }

    @JsonProperty
    public List<PixelsColumnHandle> getColumns() {
        return columns;
    }

    @JsonProperty
    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PixelsTable that = (PixelsTable) o;

        return Objects.equals(table, that.table) && Objects.equals(tableLayout, that.tableLayout)
                && Objects.equals(columns, that.columns) && Objects.equals(columnsMetadata, that.columnsMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, tableLayout, columns, columnsMetadata);
    }

    @Override
    public String toString() {
        return "PixelsTable{" +
                "table=" + table +
                ", tableLayout=" + tableLayout +
                ", columns=" + columns +
                ", columnsMetadata=" + columnsMetadata +
                '}';
    }
}
