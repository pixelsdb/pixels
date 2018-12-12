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
package cn.edu.ruc.iir.pixels.presto;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto
 * @ClassName: PixelsSplit
 * @Description:
 * @author: tao
 * @date: Create in 2018-01-20 19:15
 **/
public class PixelsSplit
        implements ConnectorSplit {
    private final Logger log = Logger.get(PixelsSplit.class);
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String path;
    private final int start;
    private final int len;
    private final boolean isCached;
    private final List<HostAddress> addresses;
    private final List<String> order;
    private final List<String> cacheOrder;
    private final TupleDomain<PixelsColumnHandle> constraint;

    @JsonCreator
    public PixelsSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("path") String path,
            @JsonProperty("start") int start,
            @JsonProperty("len") int len,
            @JsonProperty("isCached") boolean isCached,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("order") List<String> order,
            @JsonProperty("cacheOrder") List<String> cacheOrder,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint) {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.path = requireNonNull(path, "path is null");
        this.start = start;
        this.len = len;
        this.isCached = isCached;
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.order = requireNonNull(order, "order is null");
        this.cacheOrder = requireNonNull(cacheOrder, "cache order is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        // log.info("PixelsSplit Constructor:" + schemaName + ", " + tableName + ", " + path);
    }

    @JsonProperty
    public boolean isCached() {
        return isCached;
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
    public String getPath() {
        return path;
    }

    @JsonProperty
    public int getStart() {
        return start;
    }

    @JsonProperty
    public int getLen() {
        return len;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return false;
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
                Objects.equals(this.path, that.path) &&
                Objects.equals(this.start, that.start) &&
                Objects.equals(this.len, that.len) &&
                Objects.equals(this.addresses, that.addresses) &&
                Objects.equals(this.isCached, that.isCached) &&
                Objects.equals(this.constraint, that.constraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, schemaName, tableName, path, start, len, addresses, constraint);
    }

    @Override
    public String toString() {
        return "PixelsSplit{" +
                "connectorId=" + connectorId +
                ", schemaName='" + schemaName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", path='" + path + '\'' +
                ", start=" + start +
                ", len=" + len +
                ", isCached=" + isCached +
                ", addresses=" + addresses +
                '}';
    }
}
