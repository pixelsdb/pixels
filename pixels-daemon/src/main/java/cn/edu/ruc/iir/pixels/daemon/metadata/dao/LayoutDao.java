package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.daemon.MetadataProto;
import cn.edu.ruc.iir.pixels.daemon.exception.ColumnOrderException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class LayoutDao implements Dao<MetadataProto.Layout>
{
    public LayoutDao() {}

    private static Logger log = LogManager.getLogger(LayoutDao.class);

    private static final DBUtil db = DBUtil.Instance();

    @Override
    public MetadataProto.Layout getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE LAYOUT_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Layout layout = MetadataProto.Layout.newBuilder()
                .setId(id)
                .setVersion(rs.getInt("LAYOUT_VERSION"))
                .setPermission(convertPermission(rs.getShort("LAYOUT_PERMISSION")))
                .setCreateAt(rs.getLong("LAYOUT_CREATE_AT"))
                .setOrder(rs.getString("LAYOUT_ORDER"))
                .setOrderPath(rs.getString("LAYOUT_ORDER_PATH"))
                .setCompact(rs.getString("LAYOUT_COMPACT"))
                .setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"))
                .setSplits(rs.getString("LAYOUT_SPLITS"))
                .setTableId(rs.getInt("TBLS_TBL_ID")).build();
                return layout;
            }
        } catch (SQLException e)
        {
            log.error("getById in LayoutDao", e);
        }

        return null;
    }

    private MetadataProto.Layout.PERMISSION convertPermission (short permission)
    {
        switch (permission)
        {
            case -1:
                return MetadataProto.Layout.PERMISSION.DISABLED;
            case 0:
                return MetadataProto.Layout.PERMISSION.READONLY;
            case 1:
                return MetadataProto.Layout.PERMISSION.READWRITE;
        }
        return MetadataProto.Layout.PERMISSION.DISABLED;
    }

    @Override
    public List<MetadataProto.Layout> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public MetadataProto.Layout getWritableByTable(MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId() +
            " AND LAYOUT_PERMISSION>0");
            if (rs.next())
            {
                MetadataProto.Layout layout = MetadataProto.Layout.newBuilder()
                .setId(rs.getInt("LAYOUT_ID"))
                .setVersion(rs.getInt("LAYOUT_VERSION"))
                .setPermission(convertPermission(rs.getShort("LAYOUT_PERMISSION")))
                .setCreateAt(rs.getLong("LAYOUT_CREATE_AT"))
                .setOrder(rs.getString("LAYOUT_ORDER"))
                .setOrderPath(rs.getString("LAYOUT_ORDER_PATH"))
                .setCompact(rs.getString("LAYOUT_COMPACT"))
                .setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"))
                .setSplits(rs.getString("LAYOUT_SPLITS"))
                .setTableId(rs.getInt("TBLS_TBL_ID")).build();
                if (rs.next())
                {
                    throw new ColumnOrderException("multiple writable layouts founded for table: " +
                            table.getSchemaId() + "." + table.getName());
                }
                return layout;
            }
        } catch (Exception e)
        {
            log.error("getById in LayoutDao", e);
        }

        return null;
    }

    public MetadataProto.Layout getLatestByTable(MetadataProto.Table table)
    {
        List<MetadataProto.Layout> layouts = this.getByTable(table);

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

    @SuppressWarnings("Duplicates")
    public List<MetadataProto.Layout> getByTable (MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId());
            List<MetadataProto.Layout> layouts = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Layout layout = MetadataProto.Layout.newBuilder()
                .setId(rs.getInt("LAYOUT_ID"))
                .setVersion(rs.getInt("LAYOUT_VERSION"))
                .setPermission(convertPermission(rs.getShort("LAYOUT_PERMISSION")))
                .setCreateAt(rs.getLong("LAYOUT_CREATE_AT"))
                .setOrder(rs.getString("LAYOUT_ORDER"))
                .setOrderPath(rs.getString("LAYOUT_ORDER_PATH"))
                .setCompact(rs.getString("LAYOUT_COMPACT"))
                .setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"))
                .setSplits(rs.getString("LAYOUT_SPLITS"))
                .setTableId(table.getId()).build();
                layouts.add(layout);
            }
            return layouts;
        } catch (SQLException e)
        {
            log.error("getById in LayoutDao", e);
        }

        return null;
    }

    @SuppressWarnings("Duplicates")
    public List<MetadataProto.Layout> getReadableByTable (MetadataProto.Table table, int version)
    {
        if(table == null)
        {
            return null;
        }
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId() +
                    " AND LAYOUT_PERMISSION>=0";
            if(version >= 0)
            {
                sql += " AND LAYOUT_VERSION=" + version;
            }
            ResultSet rs = st.executeQuery(sql);
            List<MetadataProto.Layout> layouts = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Layout layout = MetadataProto.Layout.newBuilder()
                .setId(rs.getInt("LAYOUT_ID"))
                .setVersion(rs.getInt("LAYOUT_VERSION"))
                .setPermission(convertPermission(rs.getShort("LAYOUT_PERMISSION")))
                .setCreateAt(rs.getLong("LAYOUT_CREATE_AT"))
                .setOrder(rs.getString("LAYOUT_ORDER"))
                .setOrderPath(rs.getString("LAYOUT_ORDER_PATH"))
                .setCompact(rs.getString("LAYOUT_COMPACT"))
                .setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"))
                .setSplits(rs.getString("LAYOUT_SPLITS"))
                .setTableId(table.getId()).build();
                layouts.add(layout);
            }
            return layouts;
        } catch (SQLException e)
        {
            log.error("getById in LayoutDao", e);
        }

        return null;
    }

    public boolean save (MetadataProto.Layout layout)
    {
        if (exists(layout))
        {
            return update(layout);
        }
        else
        {
            return insert(layout);
        }
    }

    public boolean exists (MetadataProto.Layout layout)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT 1 FROM LAYOUTS WHERE LAYOUT_ID=" + layout.getId());
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in LayoutDao", e);
        }

        return false;
    }

    public boolean insert (MetadataProto.Layout layout)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO LAYOUTS(" +
                "`LAYOUT_VERSION`," +
                "`LAYOUT_CREATE_AT`," +
                "`LAYOUT_PERMISSION`," +
                "`LAYOUT_ORDER`," +
                "`LAYOUT_ORDER_PATH`," +
                "`LAYOUT_COMPACT`," +
                "`LAYOUT_COMPACT_PATH`," +
                "`LAYOUT_SPLITS`," +
                "`TBLS_TBL_ID`) VALUES (?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setInt(1, layout.getVersion());
            pst.setLong(2, layout.getCreateAt());
            pst.setInt(3, convertPermission(layout.getPermission()));
            pst.setString(4, layout.getOrder());
            pst.setString(5, layout.getOrderPath());
            pst.setString(6, layout.getCompact());
            pst.setString(7, layout.getCompactPath());
            pst.setString(8, layout.getSplits());
            pst.setLong(9, layout.getTableId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in LayoutDao", e);
        }
        return false;
    }

    private short convertPermission (MetadataProto.Layout.PERMISSION permission)
    {
        switch (permission)
        {
            case DISABLED:
                return -1;
            case READONLY:
                return 0;
            case READWRITE:
                return -1;
        }
        return -1;
    }

    public boolean update (MetadataProto.Layout layout)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE LAYOUTS\n" +
                "SET\n" +
                "`LAYOUT_VERSION` = ?," +
                "`LAYOUT_CREATE_AT` = ?," +
                "`LAYOUT_PERMISSION` = ?," +
                "`LAYOUT_ORDER` = ?," +
                "`LAYOUT_ORDER_PATH` = ?," +
                "`LAYOUT_COMPACT` = ?," +
                "`LAYOUT_COMPACT_PATH` = ?," +
                "`LAYOUT_SPLITS` = ?\n" +
                "WHERE `LAYOUT_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setInt(1, layout.getVersion());
            pst.setLong(2, layout.getCreateAt());
            pst.setInt(3, convertPermission(layout.getPermission()));
            pst.setString(4, layout.getOrder());
            pst.setString(5, layout.getOrderPath());
            pst.setString(6, layout.getCompact());
            pst.setString(7, layout.getCompactPath());
            pst.setString(8, layout.getSplits());
            pst.setLong(9, layout.getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in LayoutDao", e);
        }
        return false;
    }
}
