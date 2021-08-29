package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.etcd.jetcd.KeyValue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.TableDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdTableDao extends TableDao
{
    public EtcdTableDao() {}

    private static final EtcdUtil etcd = EtcdUtil.Instance();
    private static Logger log = LogManager.getLogger(EtcdTableDao.class);

    @Override
    public MetadataProto.Table getById(long id)
    {
        KeyValue kv = etcd.getKeyValue(EtcdCommon.tablePrimaryKeyPrefix + id);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.Table table = null;
        try
        {
            table = MetadataProto.Table.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return table;
    }

    @Override
    public MetadataProto.Table getByNameAndSchema(String name, MetadataProto.Schema schema)
    {
        KeyValue kv = etcd.getKeyValue(EtcdCommon.tableSchemaNameKeyPrefix + schema.getId() + name);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.Table table = null;
        try
        {
            table = MetadataProto.Table.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return table;
    }

    @Override
    public List<MetadataProto.Table> getByName(String name)
    {
        throw new UnsupportedOperationException("getByName is not supported.");
    }

    @Override
    public List<MetadataProto.Table> getBySchema(MetadataProto.Schema schema)
    {
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdCommon.tableSchemaNameKeyPrefix + schema.getId());
        List<MetadataProto.Table> tables = new ArrayList<>();
        for (KeyValue kv : kvs)
        {
            MetadataProto.Table table = null;
            try
            {
                table = MetadataProto.Table.parseFrom(kv.getValue().getBytes());
            } catch (InvalidProtocolBufferException e)
            {
                log.error(e);
            }
            if (table != null)
            {
                tables.add(table);
            }
        }
        return tables;
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
        KeyValue kv = etcd.getKeyValue(EtcdCommon.tablePrimaryKeyPrefix + table.getId());
        if (kv == null)
        {
            kv = etcd.getKeyValue(EtcdCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName());
            return kv != null;
        }
        return true;
    }

    @Override
    public boolean insert(MetadataProto.Table table)
    {
        long id = GenerateId(EtcdCommon.tableIdKey);
        table = table.toBuilder().setId(id).build();
        etcd.putKeyValue(EtcdCommon.tablePrimaryKeyPrefix + id,
                table.toByteArray());
        etcd.putKeyValue(EtcdCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName(),
                table.toByteArray());
        return true;
    }

    @Override
    public boolean update(MetadataProto.Table table)
    {
        etcd.putKeyValue(EtcdCommon.tablePrimaryKeyPrefix + table.getId(),
                table.toByteArray());
        etcd.putKeyValue(EtcdCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName(),
                table.toByteArray());
        return true;
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
        MetadataProto.Table table = getByNameAndSchema(name, schema);
        if (table != null)
        {
            etcd.delete(EtcdCommon.tablePrimaryKeyPrefix + table.getId());
            etcd.delete(EtcdCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName());
        }
        return true;
    }
}
