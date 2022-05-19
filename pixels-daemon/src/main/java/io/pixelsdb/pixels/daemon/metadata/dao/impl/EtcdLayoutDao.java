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
import io.pixelsdb.pixels.daemon.metadata.dao.LayoutDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdLayoutDao extends LayoutDao
{
    public EtcdLayoutDao () {}

    private static final EtcdUtil etcd = EtcdUtil.Instance();
    private static Logger log = LogManager.getLogger(EtcdLayoutDao.class);

    @Override
    public MetadataProto.Layout getById(long id)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.layoutPrimaryKeyPrefix + id);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.Layout layout = null;
        try
        {
            layout = MetadataProto.Layout.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return layout;
    }

    @Override
    public MetadataProto.Layout getLatestByRegion(MetadataProto.Region region, MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
    {
        List<MetadataProto.Layout> layouts = this.getByRegion(region, -1, permissionRange);

        MetadataProto.Layout res = null;
        if (layouts != null)
        {
            long maxId = -1;
            for (MetadataProto.Layout layout : layouts)
            {
                if (layout.getId() > maxId)
                {
                    maxId = layout.getId();
                    res = layout;
                }
            }
        }

        return res;
    }

    /**
     * get layout of a table by version and permission range.
     *
     * @param region
     * @param version         < 0 to get all versions of layouts.
     * @param permissionRange
     * @return
     */
    @Override
    public List<MetadataProto.Layout> getByRegion(MetadataProto.Region region, int version, MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
    {
        List<MetadataProto.Layout> layouts = new ArrayList<>();
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdDaoCommon.layoutTableIdKeyPrefix + region.getId());
        for (KeyValue kv : kvs)
        {
            MetadataProto.Layout layout = null;
            try
            {
                layout = MetadataProto.Layout.parseFrom(kv.getValue().getBytes());
            } catch (InvalidProtocolBufferException e)
            {
                log.error(e);
            }
            if (layout != null)
            {
                if (permissionRange != MetadataProto.GetLayoutRequest.PermissionRange.ALL)
                {
                    if (layout.getPermission().getNumber() < permissionRange.getNumber())
                    {
                        // layout permission does not meet the requirement.
                        continue;
                    }
                }
                if(version >= 0 && layout.getVersion() != version)
                {
                    // layout version does not meet the requirement.
                    continue;
                }
                layouts.add(layout);
            }
        }
        return layouts;
    }

    @Override
    public boolean exists(MetadataProto.Layout layout)
    {
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.layoutPrimaryKeyPrefix + layout.getId());
        return kv != null;
    }

    @Override
    public boolean insert(MetadataProto.Layout layout)
    {
        long id = GenerateId(EtcdDaoCommon.layoutIdKey);
        layout = layout.toBuilder().setId(id).build();
        etcd.putKeyValue(EtcdDaoCommon.layoutPrimaryKeyPrefix + id,
                layout.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.layoutTableIdKeyPrefix + layout.getRegionId() + layout.getId(),
                layout.toByteArray());
        return true;
    }

    @Override
    public boolean update(MetadataProto.Layout layout)
    {
        etcd.putKeyValue(EtcdDaoCommon.layoutPrimaryKeyPrefix + layout.getId(),
                layout.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.layoutTableIdKeyPrefix + layout.getRegionId() + layout.getId(),
                layout.toByteArray());
        return true;
    }
}
