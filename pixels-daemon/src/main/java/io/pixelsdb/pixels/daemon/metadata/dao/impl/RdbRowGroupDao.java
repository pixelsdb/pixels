package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.RowGroupDao;

import java.util.List;

public class RdbRowGroupDao extends RowGroupDao {
    public RdbRowGroupDao() {
    }

    @Override
    public MetadataProto.RowGroup getById(long id) {
        return null;
    }

    @Override
    public List<MetadataProto.RowGroup> getByLayout(MetadataProto.Layout layout) {
        return null;
    }
}
