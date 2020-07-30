package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import com.coreos.jetcd.data.KeyValue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.LayoutDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

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
        KeyValue kv = etcd.getKeyValue(EtcdCommon.layoutPrimaryKeyPrefix + id);
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
    public MetadataProto.Layout getLatestByTable(MetadataProto.Table table, MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
    {
        List<MetadataProto.Layout> layouts = this.getByTable(table, -1, permissionRange);

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
     * @param table
     * @param version         < 0 to get all versions of layouts.
     * @param permissionRange
     * @return
     */
    @Override
    public List<MetadataProto.Layout> getByTable(MetadataProto.Table table, int version, MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
    {
        List<MetadataProto.Layout> layouts = new ArrayList<>();
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdCommon.layoutTableIdKeyPrefix + table.getId());
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
        KeyValue kv = etcd.getKeyValue(EtcdCommon.layoutPrimaryKeyPrefix + layout.getId());
        return kv != null;
    }

    @Override
    public boolean insert(MetadataProto.Layout layout)
    {
        long id = EtcdCommon.generateId(EtcdCommon.layoutIdKey,
                EtcdCommon.layoutIdLockPath);
        layout = layout.toBuilder().setId(id).build();
        etcd.putKeyValue(EtcdCommon.layoutPrimaryKeyPrefix + id,
                layout.toByteArray());
        etcd.putKeyValue(EtcdCommon.layoutTableIdKeyPrefix + layout.getTableId() + layout.getId(),
                layout.toByteArray());
        return true;
    }

    @Override
    public boolean update(MetadataProto.Layout layout)
    {
        etcd.putKeyValue(EtcdCommon.layoutPrimaryKeyPrefix + layout.getId(),
                layout.toByteArray());
        etcd.putKeyValue(EtcdCommon.layoutTableIdKeyPrefix + layout.getTableId() + layout.getId(),
                layout.toByteArray());
        return true;
    }
}
