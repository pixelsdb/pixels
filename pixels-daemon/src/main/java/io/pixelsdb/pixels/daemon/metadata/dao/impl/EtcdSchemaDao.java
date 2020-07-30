package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import com.coreos.jetcd.data.KeyValue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.SchemaDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdSchemaDao extends SchemaDao
{
    public EtcdSchemaDao() {}

    private static final EtcdUtil etcd = EtcdUtil.Instance();
    private static Logger log = LogManager.getLogger(EtcdSchemaDao.class);

    @Override
    public MetadataProto.Schema getById(long id)
    {
        KeyValue kv = etcd.getKeyValue(EtcdCommon.schemaPrimaryKeyPrefix + id);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.Schema schema = null;
        try
        {
            schema = MetadataProto.Schema.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return schema;
    }

    @Override
    public List<MetadataProto.Schema> getAll()
    {
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdCommon.schemaPrimaryKeyPrefix);
        List<MetadataProto.Schema> schemas = new ArrayList<>();
        for (KeyValue kv : kvs)
        {
            MetadataProto.Schema schema = null;
            try
            {
                schema = MetadataProto.Schema.parseFrom(kv.getValue().getBytes());
            } catch (InvalidProtocolBufferException e)
            {
                log.error(e);
            }
            if (schema != null)
            {
                schemas.add(schema);
            }
        }
        return schemas;
    }

    @Override
    public MetadataProto.Schema getByName(String name)
    {
        KeyValue kv = etcd.getKeyValue(EtcdCommon.schemaNameKeyPrefix + name);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.Schema schema = null;
        try
        {
            schema = MetadataProto.Schema.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return schema;
    }

    @Override
    public boolean exists(MetadataProto.Schema schema)
    {
        KeyValue kv = etcd.getKeyValue(EtcdCommon.schemaPrimaryKeyPrefix + schema.getId());
        return kv != null;
    }

    @Override
    public boolean insert(MetadataProto.Schema schema)
    {
        long id = EtcdCommon.generateId(EtcdCommon.schemaIdKey,
                EtcdCommon.schemaIdLockPath);
        schema = schema.toBuilder().setId(id).build();
        etcd.putKeyValue(EtcdCommon.schemaPrimaryKeyPrefix + id,
                schema.toByteArray());
        etcd.putKeyValue(EtcdCommon.schemaNameKeyPrefix + schema.getName(),
                schema.toByteArray());
        return true;
    }

    @Override
    public boolean update(MetadataProto.Schema schema)
    {
        etcd.putKeyValue(EtcdCommon.schemaPrimaryKeyPrefix + schema.getId(),
                schema.toByteArray());
        etcd.putKeyValue(EtcdCommon.schemaNameKeyPrefix + schema.getName(),
                schema.toByteArray());
        return true;
    }

    /**
     * We ensure cascade delete and update in the metadata database.
     * If you delete a schema by this method, all the tables, layouts and columns of the schema
     * will be deleted.
     *
     * @param name
     * @return
     */
    @Override
    public boolean deleteByName(String name)
    {
        MetadataProto.Schema schema = getByName(name);
        if (schema != null)
        {
            etcd.delete(EtcdCommon.schemaPrimaryKeyPrefix + schema.getId());
            etcd.delete(EtcdCommon.schemaNameKeyPrefix + schema.getName());
        }
        return true;
    }
}
