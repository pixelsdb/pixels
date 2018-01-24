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

import java.net.UnknownHostException;
import java.util.List;

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
    private final long start;
    private final long len;
    private final List<HostAddress> addresses;
    private final TupleDomain<PixelsColumnHandle> constraint;

    @JsonCreator
    public PixelsSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("len") long len,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("constraint") TupleDomain<PixelsColumnHandle> constraint) {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.path = requireNonNull(path, "path is null");
        this.start = requireNonNull(start, "start is null");
        this.len = requireNonNull(len, "len is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.constraint = requireNonNull(constraint, "constraint is null");
        try {
            log.info("Address Size: " + addresses.size() + ", " + addresses.get(0).toInetAddress().toString());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        log.info("PixelsSplit Constructor:" + schemaName + ", " + tableName + ", " + path);
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
    public long getStart() {
        return start;
    }

    @JsonProperty
    public long getLen() {
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

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PixelsSplit that = (PixelsSplit) o;

        if (start != that.start) return false;
        if (len != that.len) return false;
        if (log != null ? !log.equals(that.log) : that.log != null) return false;
        if (connectorId != null ? !connectorId.equals(that.connectorId) : that.connectorId != null) return false;
        if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;
        if (path != null ? !path.equals(that.path) : that.path != null) return false;
        if (addresses != null ? !addresses.equals(that.addresses) : that.addresses != null) return false;
        return constraint != null ? constraint.equals(that.constraint) : that.constraint == null;
    }

    @Override
    public int hashCode() {
        int result = log != null ? log.hashCode() : 0;
        result = 31 * result + (connectorId != null ? connectorId.hashCode() : 0);
        result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        result = 31 * result + (path != null ? path.hashCode() : 0);
        result = 31 * result + (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (len ^ (len >>> 32));
        result = 31 * result + (addresses != null ? addresses.hashCode() : 0);
        result = 31 * result + (constraint != null ? constraint.hashCode() : 0);
        return result;
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
                ", addresses=" + addresses +
                '}';
    }
}
