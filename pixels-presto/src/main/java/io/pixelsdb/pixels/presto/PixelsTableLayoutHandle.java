/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
