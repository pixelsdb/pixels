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

import com.google.protobuf.InvalidProtocolBufferException;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.ViewDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;

/**
 * Created at: 2022/03.02
 * Author: hank
 */
public class EtcdViewDao extends ViewDao
{
    public EtcdViewDao() {}

    private static final EtcdUtil etcd = EtcdUtil.Instance();
    private static Logger log = LogManager.getLogger(EtcdViewDao.class);

    @Override
    public MetadataProto.View getById(long id)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.viewPrimaryKeyPrefix + id);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.View view = null;
        try
        {
            view = MetadataProto.View.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return view;
    }

    @Override
    public MetadataProto.View getByNameAndSchema(String name, MetadataProto.Schema schema)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.viewSchemaNameKeyPrefix + schema.getId() + name);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.View view = null;
        try
        {
            view = MetadataProto.View.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return view;
    }

    @Override
    public List<MetadataProto.View> getByName(String name)
    {
        throw new UnsupportedOperationException("getByName is not supported.");
    }

    @Override
    public List<MetadataProto.View> getBySchema(MetadataProto.Schema schema)
    {
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdDaoCommon.viewSchemaNameKeyPrefix + schema.getId());
        List<MetadataProto.View> views = new ArrayList<>();
        for (KeyValue kv : kvs)
        {
            MetadataProto.View view = null;
            try
            {
                view = MetadataProto.View.parseFrom(kv.getValue().getBytes());
            } catch (InvalidProtocolBufferException e)
            {
                log.error(e);
            }
            if (view != null)
            {
                views.add(view);
            }
        }
        return views;
    }

    @Override
    public boolean exists(MetadataProto.View view)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.viewPrimaryKeyPrefix + view.getId());
        if (kv == null)
        {
            kv = etcd.getKeyValue(EtcdDaoCommon.viewSchemaNameKeyPrefix + view.getSchemaId() + view.getName());
            return kv != null;
        }
        return true;
    }

    @Override
    public boolean insert(MetadataProto.View view)
    {
        long id = GenerateId(EtcdDaoCommon.viewIdKey);
        view = view.toBuilder().setId(id).build();
        etcd.putKeyValue(EtcdDaoCommon.viewPrimaryKeyPrefix + id, view.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.viewSchemaNameKeyPrefix + view.getSchemaId() + view.getName(),
                view.toByteArray());
        return true;
    }

    @Override
    public boolean update(MetadataProto.View view)
    {
        etcd.putKeyValue(EtcdDaoCommon.viewPrimaryKeyPrefix + view.getId(), view.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.viewSchemaNameKeyPrefix + view.getSchemaId() + view.getName(),
                view.toByteArray());
        return true;
    }

    @Override
    public boolean deleteByNameAndSchema(String name, MetadataProto.Schema schema)
    {
        MetadataProto.View view = getByNameAndSchema(name, schema);
        if (view != null)
        {
            etcd.delete(EtcdDaoCommon.viewPrimaryKeyPrefix + view.getId());
            etcd.delete(EtcdDaoCommon.viewSchemaNameKeyPrefix + view.getSchemaId() + view.getName());
        }
        return true;
    }
}
