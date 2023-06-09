package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.metadata.dao.impl.*;

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

    private ColumnDao columnDao;
    private LayoutDao layoutDao;
    private SchemaDao schemaDao;
    private TableDao tableDao;
    private ViewDao viewDao;

    private DaoFactory ()
    {
        this.columnDao = new RdbColumnDao();
        this.layoutDao = new RdbLayoutDao();
        this.schemaDao = new RdbSchemaDao();
        this.tableDao = new RdbTableDao();
        this.viewDao = new RdbViewDao();
    }

    public ColumnDao getColumnDao ()
    {
        return this.columnDao;
    }

    public LayoutDao getLayoutDao ()
    {
        return this.layoutDao;
    }

    public SchemaDao getSchemaDao ()
    {
        return this.schemaDao;
    }

    public TableDao getTableDao()
    {
        return this.tableDao;
    }

    public ViewDao getViewDao()
    {
        return this.viewDao;
    }
}
