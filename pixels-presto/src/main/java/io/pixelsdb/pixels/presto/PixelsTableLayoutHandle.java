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
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.presto;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PixelsTableLayoutHandle
        implements ConnectorTableLayoutHandle {
    private final PixelsTableHandle table;

    private TupleDomain<ColumnHandle> constraint;
    private Set<ColumnHandle> desiredColumns;

    @JsonCreator
    public PixelsTableLayoutHandle(@JsonProperty("table") PixelsTableHandle table)
    {
        this.table = requireNonNull(table, "table is null");
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    public void setConstraint(TupleDomain<ColumnHandle> constraint)
    {
        this.constraint = constraint;
    }

    @JsonProperty
    public Set<ColumnHandle> getDesiredColumns() {
        return desiredColumns;
    }

    public void setDesiredColumns(Set<ColumnHandle> desiredColumns) {
        this.desiredColumns = desiredColumns;
    }

    @JsonProperty
    public PixelsTableHandle getTable() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PixelsTableLayoutHandle that = (PixelsTableLayoutHandle) o;
        return Objects.equals(table, that.table) && Objects.equals(constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, constraint);
    }

    @Override
    public String toString() {
        return "PixelsTableLayoutHandle{" +
                "table=" + table +
                ", constraint=" + constraint +
                '}';
    }
}
