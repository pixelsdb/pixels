package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.common.utils.DBUtil2;
import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.exception.ColumnOrderException;
import cn.edu.ruc.iir.pixels.common.metadata.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.Table;
import org.apache.commons.logging.Log;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class LayoutDao implements Dao<Layout>
{
    public LayoutDao() {}

    private static final DBUtil2 db = DBUtil2.Instance();
    private static final Log log = LogFactory.Instance().getLog();
    private static final TableDao tableModel = new TableDao();

    @Override
    public Layout getById(int id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE LAYOUT_ID=" + id);
            if (rs.next())
            {
                Layout layout = new Layout();
                layout.setId(id);
                layout.setVersion(rs.getInt("LAYOUT_VERSION"));
                layout.setPermission(rs.getShort("LAYOUT_PERMISSION"));
                layout.setCreateAt(rs.getLong("LAYOUT_CREATE_AT"));
                layout.setOrder(rs.getString("LAYOUT_ORDER"));
                layout.setOrderPath(rs.getString("LAYOUT_ORDER_PATH"));
                layout.setCompact(rs.getString("LAYOUT_COMPACT"));
                layout.setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"));
                layout.setSplits(rs.getString("LAYOUT_SPLITS"));
                layout.setTable(tableModel.getById(rs.getInt("TBLS_TBL_ID")));
                return layout;
            }
        } catch (SQLException e)
        {
            log.error("getById in LayoutDao", e);
        }

        return null;
    }

    @Override
    public List<Layout> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public Layout getWritableByTable(Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId() +
            " AND LAYOUT_PERMISSION>0");
            if (rs.next())
            {
                Layout layout = new Layout();
                layout.setId(rs.getInt("LAYOUT_ID"));
                layout.setVersion(rs.getInt("LAYOUT_VERSION"));
                layout.setPermission(rs.getShort("LAYOUT_PERMISSION"));
                layout.setCreateAt(rs.getLong("LAYOUT_CREATE_AT"));
                layout.setOrder(rs.getString("LAYOUT_ORDER"));
                layout.setOrderPath(rs.getString("LAYOUT_ORDER_PATH"));
                layout.setCompact(rs.getString("LAYOUT_COMPACT"));
                layout.setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"));
                layout.setSplits(rs.getString("LAYOUT_SPLITS"));
                layout.setTable(tableModel.getById(rs.getInt("TBLS_TBL_ID")));
                if (rs.next())
                {
                    throw new ColumnOrderException("multiple writable layouts founded for table: " +
                            table.getSchema().getName() + "." + table.getName());
                }
                return layout;
            }
        } catch (Exception e)
        {
            log.error("getById in LayoutDao", e);
        }

        return null;
    }

    public Layout getLatestByTable(Table table)
    {
        List<Layout> layouts = this.getByTable(table);

        Layout res = null;
        if (layouts != null)
        {
            int maxId = -1;
            for (Layout layout : layouts)
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
    public List<Layout> getByTable (Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId());
            List<Layout> layouts = new ArrayList<>();
            while (rs.next())
            {
                Layout layout = new Layout();
                layout.setId(rs.getInt("LAYOUT_ID"));
                layout.setVersion(rs.getInt("LAYOUT_VERSION"));
                layout.setPermission(rs.getShort("LAYOUT_PERMISSION"));
                layout.setCreateAt(rs.getLong("LAYOUT_CREATE_AT"));
                layout.setOrder(rs.getString("LAYOUT_ORDER"));
                layout.setOrderPath(rs.getString("LAYOUT_ORDER_PATH"));
                layout.setCompact(rs.getString("LAYOUT_COMPACT"));
                layout.setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"));
                layout.setSplits(rs.getString("LAYOUT_SPLITS"));
                layout.setTable(table);
                table.addLayout(layout);
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
    public List<Layout> getReadableByTable (Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId() +
                    " AND LAYOUT_PERMISSION>=0");
            List<Layout> layouts = new ArrayList<>();
            while (rs.next())
            {
                Layout layout = new Layout();
                layout.setId(rs.getInt("LAYOUT_ID"));
                layout.setVersion(rs.getInt("LAYOUT_VERSION"));
                layout.setPermission(rs.getShort("LAYOUT_PERMISSION"));
                layout.setCreateAt(rs.getLong("LAYOUT_CREATE_AT"));
                layout.setOrder(rs.getString("LAYOUT_ORDER"));
                layout.setOrderPath(rs.getString("LAYOUT_ORDER_PATH"));
                layout.setCompact(rs.getString("LAYOUT_COMPACT"));
                layout.setCompactPath(rs.getString("LAYOUT_COMPACT_PATH"));
                layout.setSplits(rs.getString("LAYOUT_SPLITS"));
                layout.setTable(table);
                table.addLayout(layout);
                layouts.add(layout);
            }
            return layouts;
        } catch (SQLException e)
        {
            log.error("getById in LayoutDao", e);
        }

        return null;
    }

    public boolean save (Layout layout)
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

    public boolean exists (Layout layout)
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

    private boolean insert (Layout layout)
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
            pst.setInt(3, layout.getPermission());
            pst.setString(4, layout.getOrder());
            pst.setString(5, layout.getOrderPath());
            pst.setString(6, layout.getCompact());
            pst.setString(7, layout.getCompactPath());
            pst.setString(8, layout.getSplits());
            pst.setInt(9, layout.getTable().getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in LayoutDao", e);
        }
        return false;
    }

    private boolean update (Layout layout)
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
            pst.setInt(3, layout.getPermission());
            pst.setString(4, layout.getOrder());
            pst.setString(5, layout.getOrderPath());
            pst.setString(6, layout.getCompact());
            pst.setString(7, layout.getCompactPath());
            pst.setString(8, layout.getSplits());
            pst.setInt(9, layout.getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("insert in LayoutDao", e);
        }
        return false;
    }
}
