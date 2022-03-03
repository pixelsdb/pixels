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
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.ColumnDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;

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
        KeyValue kv = etcd.getKeyValue(EtcdDaoCommon.columnPrimaryKeyPrefix + id);
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
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdDaoCommon.columnTableNameKeyPrefix + table.getId());
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
        List<KeyValue> kvs = etcd.getKeyValuesByPrefix(EtcdDaoCommon.columnTableNameKeyPrefix + table.getId());
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
        etcd.putKeyValue(EtcdDaoCommon.columnPrimaryKeyPrefix + column.getId(),
                column.toByteArray());
        etcd.putKeyValue(EtcdDaoCommon.columnTableNameKeyPrefix + column.getTableId() + column.getName(),
                column.toByteArray());
        return true;
    }

    @Override
    public int insertBatch(MetadataProto.Table table, List<MetadataProto.Column> columns)
    {
        int n = 0;
        for (MetadataProto.Column column : columns)
        {
            long id = GenerateId(EtcdDaoCommon.columnIdKey);
            MetadataProto.Column newColumn = column.toBuilder().setId(id).setTableId(table.getId()).build();
            etcd.putKeyValue(EtcdDaoCommon.columnPrimaryKeyPrefix + newColumn.getId(),
                    newColumn.toByteArray());
            etcd.putKeyValue(EtcdDaoCommon.columnTableNameKeyPrefix + newColumn.getTableId() + newColumn.getName(),
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
            etcd.delete(EtcdDaoCommon.columnPrimaryKeyPrefix + column.getId());
        }
        etcd.deleteByPrefix(EtcdDaoCommon.columnTableNameKeyPrefix + table.getId());
        return true;
    }
}
