package io.pixelsdb.pixels.daemon.metadata.dao;

import io.pixelsdb.pixels.daemon.metadata.dao.impl.*;

/**
 * @author hank
 * @create 2019-10-16
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

    private final ColumnDao columnDao;
    private final LayoutDao layoutDao;
    private final SchemaDao schemaDao;
    private final TableDao tableDao;
    private final ViewDao viewDao;
    private final PathDao pathDao;
    private final FileDao fileDao;
    private final PeerDao peerDao;
    private final PeerPathDao peerPathDao;
    private final SchemaVersionDao schemaVersionDao;
    private final RangeDao rangeDao;
    private final RangeIndexDao rangeIndexDao;

    private DaoFactory ()
    {
        this.columnDao = new RdbColumnDao();
        this.layoutDao = new RdbLayoutDao();
        this.schemaDao = new RdbSchemaDao();
        this.tableDao = new RdbTableDao();
        this.viewDao = new RdbViewDao();
        this.pathDao = new RdbPathDao();
        this.fileDao = new RdbFileDao();
        this.peerDao = new RdbPeerDao();
        this.peerPathDao = new RdbPeerPathDao();
        this.schemaVersionDao = new RdbSchemaVersionDao();
        this.rangeDao = new RdbRangeDao();
        this.rangeIndexDao = new RdbRangeIndexDao();
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

    public PathDao getPathDao()
    {
        return this.pathDao;
    }

    public FileDao getFileDao()
    {
        return this.fileDao;
    }

    public PeerDao getPeerDao()
    {
        return this.peerDao;
    }

    public PeerPathDao getPeerPathDao()
    {
        return this.peerPathDao;
    }

    public SchemaVersionDao getSchemaVersionDao()
    {
        return this.schemaVersionDao;
    }

    public RangeDao getRangeDao()
    {
        return rangeDao;
    }

    public RangeIndexDao getRangeIndexDao()
    {
        return rangeIndexDao;
    }
}
