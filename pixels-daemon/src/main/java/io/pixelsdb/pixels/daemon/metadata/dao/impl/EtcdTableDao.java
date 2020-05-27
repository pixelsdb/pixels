package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.TableDao;

import java.util.List;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdTableDao extends TableDao
{
    @Override
    public MetadataProto.Table getById(long id)
    {
        return null;
    }

    @Override
    public MetadataProto.Table getByNameAndSchema(String name, MetadataProto.Schema schema)
    {
        return null;
    }

    @Override
    public List<MetadataProto.Table> getByName(String name)
    {
        return null;
    }

    @Override
    public List<MetadataProto.Table> getBySchema(MetadataProto.Schema schema)
    {
        return null;
    }

    /**
     * If the table with the same id or with the same db_id and table name exists,
     * this method returns false.
     *
     * @param table
     * @return
     */
    @Override
    public boolean exists(MetadataProto.Table table)
    {
        return false;
    }

    @Override
    public boolean insert(MetadataProto.Table table)
    {
        return false;
    }

    @Override
    public boolean update(MetadataProto.Table table)
    {
        return false;
    }

    /**
     * We ensure cascade delete and update in the metadata database.
     * If you delete a table by this method, all the layouts and columns of the table
     * will be deleted.
     *
     * @param name
     * @param schema
     * @return
     */
    @Override
    public boolean deleteByNameAndSchema(String name, MetadataProto.Schema schema)
    {
        return false;
    }
}
