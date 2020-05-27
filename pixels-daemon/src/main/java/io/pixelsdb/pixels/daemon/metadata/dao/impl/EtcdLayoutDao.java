package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.LayoutDao;

import java.util.List;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdLayoutDao extends LayoutDao
{
    @Override
    public MetadataProto.Layout getById(long id)
    {
        return null;
    }

    @Override
    public MetadataProto.Layout getLatestByTable(MetadataProto.Table table, MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
    {
        return null;
    }

    /**
     * get layout of a table by version and permission range.
     *
     * @param table
     * @param version         < 0 to get all versions of layouts.
     * @param permissionRange
     * @return
     */
    @Override
    public List<MetadataProto.Layout> getByTable(MetadataProto.Table table, int version, MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
    {
        return null;
    }

    @Override
    public boolean exists(MetadataProto.Layout layout)
    {
        return false;
    }

    @Override
    public boolean insert(MetadataProto.Layout layout)
    {
        return false;
    }

    @Override
    public boolean update(MetadataProto.Layout layout)
    {
        return false;
    }
}
