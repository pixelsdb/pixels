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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
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
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String path;
    private final long start;
    private final long len;
    private final List<HostAddress> addresses;

    @JsonCreator
    public PixelsSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("path") String path,
            @JsonProperty("start") long start,
            @JsonProperty("len") long len,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.path = requireNonNull(path, "path is null");
        this.start = requireNonNull(start, "start is null");
        this.len = requireNonNull(len, "len is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
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

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
