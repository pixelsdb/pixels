package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import com.coreos.jetcd.data.KeyValue;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.ColumnDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdColumnDao extends ColumnDao
{
    public EtcdColumnDao () {}

    private static final EtcdUtil etcd = EtcdUtil.Instance();
    private static Logger log = LogManager.getLogger(EtcdColumnDao.class);

    @Override
    public MetadataProto.Column getById(long id)
    {
        KeyValue kv = etcd.getKeyValue(EtcdCommon.columnPrimaryKeyPrefix + id);
        if (kv == null)
        {
            return null;
        }
        MetadataProto.Column column = null;
        try
        {
            column = MetadataProto.Column.parseFrom(kv.getValue().getBytes());
        } catch (InvalidProtocolBufferException e)
        {
            log.error(e);
        }
        return column;
    }

    @Override
    public List<MetadataProto.Column> getByTable(MetadataProto.Table table)
    {
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdCommon.columnTableNameKeyPrefix + table.getId());
        List<MetadataProto.Column> columns = new ArrayList<>();
        for (KeyValue kv : kvs)
        {
            MetadataProto.Column column = null;
            try
            {
                column = MetadataProto.Column.parseFrom(kv.getValue().getBytes());
            } catch (InvalidProtocolBufferException e)
            {
                log.error(e);
            }
            if (column != null)
            {
                columns.add(column);
            }
        }
        return columns;
    }

    @Override
    public Order getOrderByTable(MetadataProto.Table table)
    {
        Order columnOrder = new Order();
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdCommon.columnTableNameKeyPrefix + table.getId());
        List<String> columns = new ArrayList<>();
        for (KeyValue kv : kvs)
        {
            MetadataProto.Column column = null;
            try
            {
                column = MetadataProto.Column.parseFrom(kv.getValue().getBytes());
            } catch (InvalidProtocolBufferException e)
            {
                log.error(e);
            }
            if (column != null)
            {
                columns.add(column.getName());
            }
        }
        columnOrder.setColumnOrder(columns);
        return columnOrder;
    }

    @Override
    public boolean update(MetadataProto.Column column)
    {
        etcd.putKeyValue(EtcdCommon.columnPrimaryKeyPrefix + column.getId(),
                column.toByteArray());
        etcd.putKeyValue(EtcdCommon.columnTableNameKeyPrefix + column.getTableId() + column.getName(),
                column.toByteArray());
        return true;
    }

    @Override
    public int insertBatch(MetadataProto.Table table, List<MetadataProto.Column> columns)
    {
        int n = 0;
        for (MetadataProto.Column column : columns)
        {
            long id = EtcdCommon.generateId(EtcdCommon.columnIdKey,
                    EtcdCommon.columnIdLockPath);
            MetadataProto.Column newColumn = column.toBuilder().setId(id).setTableId(table.getId()).build();
            etcd.putKeyValue(EtcdCommon.columnPrimaryKeyPrefix + newColumn.getId(),
                    newColumn.toByteArray());
            etcd.putKeyValue(EtcdCommon.columnTableNameKeyPrefix + newColumn.getTableId() + newColumn.getName(),
                    newColumn.toByteArray());
            n++;
        }
        return n;
    }

    @Override
    public boolean deleteByTable(MetadataProto.Table table)
    {
        List<MetadataProto.Column> columns = getByTable(table);
        for (MetadataProto.Column column : columns)
        {
            etcd.delete(EtcdCommon.columnPrimaryKeyPrefix + column.getId());
        }
        etcd.deleteByPrefix(EtcdCommon.columnTableNameKeyPrefix + table.getId());
        return true;
    }
}
