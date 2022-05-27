package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.metadata.dao.impl.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created at: 19-10-16
 * Author: hank
 */
public class DaoFactory
{
    private static DaoFactory instance = null;

    public static DaoFactory Instance ()
    {
        if (instance == null)
        {
            instance = new DaoFactory();
        }
        return instance;
    }

    private Map<String, ColumnDao> columnDaoMap = new HashMap<>();
    private Map<String, LayoutDao> layoutDaoMap = new HashMap<>();
    private Map<String, SchemaDao> schemaDaoMap = new HashMap<>();
    private Map<String, TableDao> tableDaoMap = new HashMap<>();
    private Map<String, ViewDao> viewDaoMap = new HashMap<>();
    private Map<String, RegionDao> regionDaoMap = new HashMap<>();
    private Map<String, RowGroupDao> rowGroupDaoMap = new HashMap<>();

    private DaoFactory ()
    {
        this.columnDaoMap.put("rdb", new RdbColumnDao());
        this.layoutDaoMap.put("rdb", new RdbLayoutDao());
        this.schemaDaoMap.put("rdb", new RdbSchemaDao());
        this.tableDaoMap.put("rdb", new RdbTableDao());
        this.viewDaoMap.put("rdb", new RdbViewDao());
        this.regionDaoMap.put("rdb", new RdbRegionDao());
        this.rowGroupDaoMap.put("rdb", new RdbRowGroupDao());
    }

    public ColumnDao getColumnDao (String type)
    {
        return this.columnDaoMap.get(type);
    }

    public LayoutDao getLayoutDao (String type)
    {
        return this.layoutDaoMap.get(type);
    }

    public SchemaDao getSchemaDao (String type)
    {
        return this.schemaDaoMap.get(type);
    }

    public TableDao getTableDao(String type)
    {
        return this.tableDaoMap.get(type);
    }

    public ViewDao getViewDao(String type)
    {
        return this.viewDaoMap.get(type);
    }

    public RegionDao getRegionDao(String type)
    {
        return this.regionDaoMap.get(type);
    }

    public RowGroupDao getRowGroupDao(String type)
    {
        return this.rowGroupDaoMap.get(type);
    }
}
