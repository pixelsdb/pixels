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

import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.daemon.MetadataProto;

/**
 * The information of peer, i.e., an edge (client-side) storage.
 * @author hank
 * @create 2023-06-09
 */
public class Peer extends Base
{
    private String name;
    private String location;
    private String host;
    private int port;
    private Storage.Scheme storageScheme;

    public Peer() { }

    public Peer(MetadataProto.Peer peer)
    {
        this.setId(peer.getId());
        this.name = peer.getName();
        this.location = peer.getLocation();
        this.host = peer.getHost();
        this.port = peer.getPort();
        this.storageScheme = Storage.Scheme.from(peer.getStorageScheme());
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public String getLocation()
    {
        return location;
    }

    public void setLocation(String location)
    {
        this.location = location;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public Storage.Scheme getStorageScheme()
    {
        return storageScheme;
    }

    public void setStorageScheme(Storage.Scheme storageScheme)
    {
        this.storageScheme = storageScheme;
    }

    @Override
    public String toString()
    {
        return "Peer{" +
                "peerId='" + this.getId() + '\'' +
                ", name='" + name + '\'' +
                ", location='" + location + '\'' +
                ", host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", storageScheme='" + storageScheme + '\'' + '}';
    }
}
