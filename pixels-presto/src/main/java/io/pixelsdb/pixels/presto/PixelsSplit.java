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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.transaction.QueryTransInfo;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author: tao, hank
 * @date: Create in 2018-01-20 19:15
 **/
public class PixelsSplit
        implements ConnectorSplit {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String storageScheme;
    private final List<String> paths;
    private final QueryTransInfo transInfo;
    private final int start;
    private final int len;
    private int pathIndex;
    private boolean cached;
    private final List<HostAddress> addresses;
    private final List<String> order;
    private final List<String> cacheOrder;
    private final TupleDomain<PixelsColumnHandle> constraint;

    @JsonCreator
    public PixelsSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("storageScheme") String storageScheme,
            @JsonProperty("paths") List<String> paths,
            @JsonProperty("transInfo") QueryTransInfo transInfo,
            @JsonProperty("start") int start,
            @JsonProperty("len") int len,
            @JsonProperty("cached") boolean cached,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("order") List<String> order,
            @JsonProperty("cacheOrder") List<String> cacheOrder,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint) {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.storageScheme = requireNonNull(storageScheme, "storage scheme is null");
        this.paths = requireNonNull(paths, "paths is null");
        if (paths.isEmpty())
        {
            throw new NullPointerException("paths is empty");
        }
        this.pathIndex = 0;
        this.transInfo = transInfo;
        this.start = start;
        this.len = len;
        this.cached = cached;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.order = requireNonNull(order, "order is null");
        this.cacheOrder = requireNonNull(cacheOrder, "cache order is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public TupleDomain<PixelsColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getStorageScheme() {
        return storageScheme;
    }

    @JsonProperty
    public List<String> getPaths() {
        return paths;
    }

    @JsonProperty
    public QueryTransInfo getTransInfo() {
        return transInfo;
    }

    @JsonProperty
    public int getStart() {
        return start;
    }

    @JsonProperty
    public int getLen() {
        return len;
    }

    @JsonProperty
    public boolean getCached()
    {
        return cached;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    public boolean nextPath()
    {
        if (this.pathIndex+1 < this.paths.size())
        {
            this.pathIndex++;
            return true;
        }
        else
        {
            return false;
        }
    }

    public String getPath()
    {
        return this.paths.get(this.pathIndex);
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @JsonProperty
    public List<String> getOrder()
    {
        return order;
    }

    @JsonProperty
    public List<String> getCacheOrder()
    {
        return cacheOrder;
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PixelsSplit that = (PixelsSplit) o;

        return Objects.equals(this.connectorId, that.connectorId) &&
                Objects.equals(this.schemaName, that.schemaName) &&
                Objects.equals(this.tableName, that.tableName) &&
                Objects.equals(this.paths, that.paths) &&
                Objects.equals(this.start, that.start) &&
                Objects.equals(this.len, that.len) &&
                Objects.equals(this.addresses, that.addresses) &&
                Objects.equals(this.cached, that.cached) &&
                Objects.equals(this.constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, schemaName, tableName, paths, start, len, addresses, cached, constraint);
    }

    @Override
    public String toString() {
        StringBuilder pathBuilder = new StringBuilder("{");
        if (paths.isEmpty() == false)
        {
            pathBuilder.append(paths.get(0));
            for (int i = 1; i < paths.size(); ++i)
            {
                pathBuilder.append(",").append(paths.get(i));
            }
        }
        pathBuilder.append("}");
        return "PixelsSplit{" +
                "connectorId=" + connectorId +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", storageScheme='" + storageScheme + '\'' +
                ", paths=" + pathBuilder.toString() +
                ", start=" + start +
                ", len=" + len +
                ", isCached=" + cached +
                ", addresses=" + addresses +
                '}';
    }
}
