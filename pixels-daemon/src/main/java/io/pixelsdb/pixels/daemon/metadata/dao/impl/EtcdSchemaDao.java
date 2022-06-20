/*
 * Copyright 2020 PixelsDB.
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
package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import io.etcd.jetcd.KeyValue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.SchemaDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdSchemaDao extends SchemaDao
{
    public EtcdSchemaDao() {}

    private static final EtcdUtil etcd = EtcdUtil.Instance();
    private static final Logger log = LogManager.getLogger(EtcdSchemaDao.class);

    @Override
    public MetadataProto.Schema getById(long id)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.schemaPrimaryKeyPrefix + id);
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
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdDaoCommon.schemaPrimaryKeyPrefix);
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
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.schemaNameKeyPrefix + name);
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
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.schemaPrimaryKeyPrefix + schema.getId());
        if (kv == null)
        {
            kv = etcd.getKeyValue(EtcdDaoCommon.schemaNameKeyPrefix + schema.getName());
            return kv != null;
        }
        return true;
    }

    @Override
    public boolean insert(MetadataProto.Schema schema)
    {
        long id = GenerateId(EtcdDaoCommon.schemaIdKey);
        schema = schema.toBuilder().setId(id).build();
        etcd.putKeyValue(EtcdDaoCommon.schemaPrimaryKeyPrefix + id,
                schema.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.schemaNameKeyPrefix + schema.getName(),
                schema.toByteArray());
        return true;
    }

    @Override
    public boolean update(MetadataProto.Schema schema)
    {
        etcd.putKeyValue(EtcdDaoCommon.schemaPrimaryKeyPrefix + schema.getId(),
                schema.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.schemaNameKeyPrefix + schema.getName(),
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
            etcd.delete(EtcdDaoCommon.schemaPrimaryKeyPrefix + schema.getId());
            etcd.delete(EtcdDaoCommon.schemaNameKeyPrefix + schema.getName());
        }
        return true;
    }
}
