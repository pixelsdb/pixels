/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.common.metadata.domain;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.daemon.MetadataProto;

/**
 * @author hank
 * @create 2023-06-09
 */
public class PeerPath
{
    private String uri;
    private Columns columns;
    private long pathId;
    private long peerId;
    private String columnsJson;

    public PeerPath() { }

    public PeerPath(MetadataProto.PeerPath proto)
    {
        this.uri = proto.getUri();
        this.columnsJson = proto.getColumns();
        this.columns = JSON.parseObject(this.columnsJson, Columns.class);
        this.pathId = proto.getPathId();
        this.peerId = proto.getPeerId();
    }

    public String getUri()
    {
        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public long getPathId()
    {
        return pathId;
    }

    public void setPathId(long pathId)
    {
        this.pathId = pathId;
    }

    public long getPeerId()
    {
        return peerId;
    }

    public void setPeerId(long peerId)
    {
        this.peerId = peerId;
    }

    public Columns getColumns()
    {
        if (this.columns == null)
        {
            this.columns = JSON.parseObject(this.columnsJson, Columns.class);
        }
        return this.columns;
    }

    public String getColumnsJson()
    {
        return columnsJson;
    }

    public void setColumnsJson(String columnsJson)
    {
        this.columnsJson = columnsJson;
    }
}
