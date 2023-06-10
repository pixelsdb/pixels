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
package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

/**
 * @author hank
 * @create 2023-06-09
 */
public abstract class PeerDao implements Dao<MetadataProto.Peer>
{
    @Override
    public abstract MetadataProto.Peer getById(long id);

    @Override
    public abstract List<MetadataProto.Peer> getAll();

    public abstract MetadataProto.Peer getByName(String name);

    public boolean save (MetadataProto.Peer peer)
    {
        if (exists(peer))
        {
            return update(peer);
        }
        else
        {
            return insert(peer);
        }
    }

    abstract public boolean exists (MetadataProto.Peer peer);

    abstract public boolean insert (MetadataProto.Peer peer);

    abstract public boolean update (MetadataProto.Peer peer);

    abstract public boolean deleteById (long id);
}
