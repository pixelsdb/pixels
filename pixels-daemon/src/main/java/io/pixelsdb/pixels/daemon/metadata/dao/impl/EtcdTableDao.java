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
    private static final Logger log = LogManager.getLogger(EtcdTableDao.class);

    @Override
    public MetadataProto.Table getById(long id)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.tablePrimaryKeyPrefix + id);
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
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.tableSchemaNameKeyPrefix + schema.getId() + name);
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
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdDaoCommon.tableSchemaNameKeyPrefix + schema.getId());
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

    @Override
    public boolean exists(MetadataProto.Table table)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.tablePrimaryKeyPrefix + table.getId());
        if (kv == null)
        {
            kv = etcd.getKeyValue(EtcdDaoCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName());
            return kv != null;
        }
        return true;
    }

    @Override
    public boolean insert(MetadataProto.Table table)
    {
        long id = GenerateId(EtcdDaoCommon.tableIdKey);
        table = table.toBuilder().setId(id).build();
        etcd.putKeyValue(EtcdDaoCommon.tablePrimaryKeyPrefix + id,
                table.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName(),
                table.toByteArray());
        return true;
    }

    @Override
    public boolean update(MetadataProto.Table table)
    {
        etcd.putKeyValue(EtcdDaoCommon.tablePrimaryKeyPrefix + table.getId(),
                table.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName(),
                table.toByteArray());
        return true;
    }

    @Override
    public boolean deleteByNameAndSchema(String name, MetadataProto.Schema schema)
    {
        MetadataProto.Table table = getByNameAndSchema(name, schema);
        if (table != null)
        {
            etcd.delete(EtcdDaoCommon.tablePrimaryKeyPrefix + table.getId());
            etcd.delete(EtcdDaoCommon.tableSchemaNameKeyPrefix + table.getSchemaId() + table.getName());
        }
        return true;
    }
}
