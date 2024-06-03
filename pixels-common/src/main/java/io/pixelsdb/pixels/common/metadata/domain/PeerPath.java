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

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author hank
 * @create 2023-06-09
 */
public class PeerPath extends Base
{
    private String uri;
    private List<Column> columns;
    private long pathId;
    private long peerId;

    public PeerPath() { }

    public PeerPath(MetadataProto.PeerPath peerPath)
    {
        this.setId(peerPath.getId());
        this.uri = peerPath.getUri();
        this.columns = Column.convertColumns(peerPath.getColumnsList());
        this.pathId = peerPath.getPathId();
        this.peerId = peerPath.getPeerId();
    }

    public static List<PeerPath> convertPeerPaths(List<MetadataProto.PeerPath> protoPeerPaths)
    {
        requireNonNull(protoPeerPaths, "protoPeerPaths is null");
        ImmutableList.Builder<PeerPath> pathsBuilder =
                ImmutableList.builderWithExpectedSize(protoPeerPaths.size());
        for (MetadataProto.PeerPath protoPeerPath : protoPeerPaths)
        {
            pathsBuilder.add(new PeerPath(protoPeerPath));
        }
        return pathsBuilder.build();
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

    public List<Column> getColumns()
    {
        return this.columns;
    }

    public void setColumns(List<Column> columns)
    {
        this.columns = columns;
    }

    @Override
    public String toString()
    {
        return "PeerPath{" +
                "peerPathId='" + this.getId() + '\'' +
                ", uri='" + uri + '\'' +
                ", columns='" + columns + '\'' +
                ", pathId='" + pathId + '\'' +
                ", peerId='" + peerId + '\'' + '}';
    }

    @Override
    public MetadataProto.PeerPath toProto()
    {
        return MetadataProto.PeerPath.newBuilder().setId(this.getId()).setUri(this.uri)
                .addAllColumns(Column.revertColumns(this.columns))
                .setPathId(this.pathId).setPeerId(this.peerId).build();
    }
}
