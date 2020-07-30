package io.pixelsdb.pixels.daemon.metadata.dao.impl;

import com.coreos.jetcd.data.KeyValue;
import io.pixelsdb.pixels.common.lock.EtcdMutex;
import io.pixelsdb.pixels.common.lock.EtcdReadWriteLock;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created at: 2020/5/27
 * Author: hank
 */
public class EtcdCommon
{
    // prefix + schema id
    public static final String schemaPrimaryKeyPrefix = "pixels_meta_schema_primary_";
    // prefix + schema name
    public static final String schemaNameKeyPrefix = "pixels_meta_schema_name_";
    // prefix + table id
    public static final String tablePrimaryKeyPrefix = "pixels_meta_table_primary_";
    // prefix + schema id + table name
    public static final String tableSchemaNameKeyPrefix = "pixels_meta_table_schema_name_";
    // prefix + column id
    public static final String columnPrimaryKeyPrefix = "pixels_meta_column_primary_";
    // prefix + table id + column name
    public static final String columnTableNameKeyPrefix = "pixels_meta_column_table_name_";
    // prefix + layout id
    public static final String layoutPrimaryKeyPrefix = "pixels_meta_layout_primary_";
    // prefix + table id + layout id
    public static final String layoutTableIdKeyPrefix = "pixels_meta_layout_table_";

    // used for generating ids for schemas, tables, layouts, and columns.
    public static final String schemaIdLockPath = "/pixels_meta/schema_id_lock";
    public static final String schemaIdKey = "pixels_meta_schema_id";
    public static final String tableIdLockPath = "/pixels_meta/table_id_lock";
    public static final String tableIdKey = "pixels_meta_table_id";
    public static final String layoutIdLockPath = "/pixels_meta/layout_id_lock";
    public static final String layoutIdKey = "pixels_meta_layout_id";
    public static final String columnIdLockPath = "/pixels_meta/column_id_lock";
    public static final String columnIdKey = "pixels_meta_column_id";

    static
    {
        initIdKey(schemaIdKey, schemaIdLockPath);
        initIdKey(tableIdKey, tableIdLockPath);
        initIdKey(layoutIdKey, layoutIdLockPath);
        initIdKey(columnIdKey, columnIdLockPath);
    }

    private static void initIdKey(String idKey, String idLockPath)
    {
        EtcdUtil etcd = EtcdUtil.Instance();
        Logger log = LogManager.getLogger(EtcdCommon.class);
        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                idLockPath);
        EtcdMutex writeLock = readWriteLock.writeLock();
        try
        {
            writeLock.acquire();
            KeyValue idKV = etcd.getKeyValue(idKey);
            if (idKV == null)
            {
                etcd.putKeyValue(idKey, "0");
            }
        } catch (Exception e)
        {
            log.error(e);
        } finally
        {
            try
            {
                writeLock.release();
            } catch (Exception e)
            {
                log.error(e);
            }
        }
    }

    public static long generateId(String idKey, String idLockPath)
    {
        long id = 0;
        EtcdUtil etcd = EtcdUtil.Instance();
        Logger log = LogManager.getLogger(EtcdCommon.class);
        EtcdReadWriteLock readWriteLock = new EtcdReadWriteLock(etcd.getClient(),
                idLockPath);
        EtcdMutex writeLock = readWriteLock.writeLock();
        try
        {
            writeLock.acquire();
            KeyValue idKV = etcd.getKeyValue(idKey);
            if (idKV != null)
            {
                id = Long.parseLong(idKV.getValue().toString());
                id++;
                etcd.putKeyValue(idKey, id + "");
            }
        } catch (Exception e)
        {
            log.error(e);
        } finally
        {
            try
            {
                writeLock.release();
            } catch (Exception e)
            {
                log.error(e);
            }
        }
        return id;
    }
}
