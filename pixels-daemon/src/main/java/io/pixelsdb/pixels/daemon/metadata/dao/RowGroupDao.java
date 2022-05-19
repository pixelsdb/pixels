package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.MetadataProto;

import java.util.List;

public abstract class RowGroupDao implements Dao<MetadataProto.RowGroup> {
    public RowGroupDao() {
    }

    @Override
    abstract public MetadataProto.RowGroup getById(long id);

    @Override
    public List<MetadataProto.RowGroup> getAll() {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    abstract public List<MetadataProto.RowGroup> getByLayout(MetadataProto.Layout layout);
}
