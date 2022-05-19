package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

public abstract class RegionDao implements Dao<MetadataProto.Region> {
    public RegionDao() {
    }

    @Override
    abstract public MetadataProto.Region getById(long id);

    @Override
    public List<MetadataProto.Region> getAll() {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    abstract public List<MetadataProto.Region> getByTable(MetadataProto.Table table);
}
