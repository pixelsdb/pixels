/*
 * Copyright 2019 PixelsDB.
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

import io.pixelsdb.pixels.common.utils.DBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.LayoutDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 */
public class RdbLayoutDao extends LayoutDao
{
    public RdbLayoutDao() {}

    private static Logger log = LogManager.getLogger(RdbLayoutDao.class);

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
            log.error("getById in RdbLayoutDao", e);
        }

        return null;
    }

    public MetadataProto.Layout getLatestByTable(MetadataProto.Table table,
                                                 MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
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
     * @param table
     * @param version < 0 to get all versions of layouts.
     * @return
     */
    public List<MetadataProto.Layout> getByTable (MetadataProto.Table table, int version,
                                                          MetadataProto.GetLayoutRequest.PermissionRange permissionRange)
    {
        if(table == null)
        {
            return null;
        }
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT * FROM LAYOUTS WHERE TBLS_TBL_ID=" + table.getId();
            if (permissionRange == MetadataProto.GetLayoutRequest.PermissionRange.READABLE)
            {
                sql += " AND LAYOUT_PERMISSION>=0";
            }
            else if (permissionRange == MetadataProto.GetLayoutRequest.PermissionRange.READ_WRITE)
            {
                sql += " AND LAYOUT_PERMISSION>=1";
            }
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
            log.error("getById in RdbLayoutDao", e);
        }

        return null;
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
            log.error("exists in RdbLayoutDao", e);
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
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbLayoutDao", e);
        }
        return false;
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
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbLayoutDao", e);
        }
        return false;
    }
}
