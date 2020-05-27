package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.ColumnDao;

import java.util.List;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdColumnDao extends ColumnDao
{
    @Override
    public MetadataProto.Column getById(long id)
    {
        return null;
    }

    @Override
    public List<MetadataProto.Column> getByTable(MetadataProto.Table table)
    {
        return null;
    }

    @Override
    public Order getOrderByTable(MetadataProto.Table table)
    {
        return null;
    }

    @Override
    public boolean update(MetadataProto.Column column)
    {
        return false;
    }

    @Override
    public int insertBatch(MetadataProto.Table table, List<MetadataProto.Column> columns)
    {
        return 0;
    }

    @Override
    public boolean deleteByTable(MetadataProto.Table table)
    {
        return false;
    }
}
