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

import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.LayoutDao;
import io.pixelsdb.pixels.daemon.metadata.dao.PathDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static io.pixelsdb.pixels.common.metadata.domain.Permission.convertPermission;

/**
 * @author hank
 */
public class RdbLayoutDao extends LayoutDao
{
    private static final Logger log = LogManager.getLogger(RdbLayoutDao.class);

    public RdbLayoutDao() {}

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    // Issue #1105: do not get pathDao from DaoFactory, otherwise it leads to recursive initialization of DaoFactory.
    private static final PathDao pathDao = new RdbPathDao();

    @Override
    public MetadataProto.Layout getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM LAYOUTS WHERE LAYOUT_ID=" + id);
            if (rs.next())
            {
                List<MetadataProto.Path> orderedPaths = new ArrayList<>();
                List<MetadataProto.Path> compactPaths = new ArrayList<>();
                List<MetadataProto.Path> projectionPaths = new ArrayList<>();
                this.splitPaths(pathDao.getAllByLayoutId(id), orderedPaths, compactPaths, projectionPaths);
                MetadataProto.Layout layout = MetadataProto.Layout.newBuilder()
                        .setId(id)
                        .setVersion(rs.getLong("LAYOUT_VERSION"))
                        .setPermission(convertPermission(rs.getShort("LAYOUT_PERMISSION")))
                        .setCreateAt(rs.getLong("LAYOUT_CREATE_AT"))
                        .setOrdered(rs.getString("LAYOUT_ORDERED"))
                        .addAllOrderedPaths(orderedPaths)
                        .setCompact(rs.getString("LAYOUT_COMPACT"))
                        .addAllCompactPaths(compactPaths)
                        .setSplits(rs.getString("LAYOUT_SPLITS"))
                        .setProjections(rs.getString("LAYOUT_PROJECTIONS"))
                        .addAllProjectionPaths(projectionPaths)
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID"))
                        .setTableId(rs.getLong("TBLS_TBL_ID")).build();
                return layout;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbLayoutDao", e);
        }

        return null;
    }

    /**
     * Split the paths of this layout into the compact, layout, and projection paths.
     * @param sourcePaths the paths of this layout
     * @param orderedPaths the ordered paths list
     * @param compactPaths the compact paths list
     * @param projectionPaths the projection paths list
     */
    private void splitPaths(List<MetadataProto.Path> sourcePaths, List<MetadataProto.Path> orderedPaths,
                            List<MetadataProto.Path> compactPaths, List<MetadataProto.Path> projectionPaths)
    {
        for (MetadataProto.Path path : sourcePaths)
        {
            if (path.getType() == MetadataProto.Path.Type.COMPACT)
            {
                compactPaths.add(path);
            }
            else if (path.getType() == MetadataProto.Path.Type.ORDERED)
            {
                orderedPaths.add(path);
            }
            else if (path.getType() == MetadataProto.Path.Type.PROJECTION)
            {
                projectionPaths.add(path);
            }
        }
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
    public List<MetadataProto.Layout> getByTable (MetadataProto.Table table, long version,
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
                long layoutId = rs.getLong("LAYOUT_ID");
                List<MetadataProto.Path> orderedPaths = new ArrayList<>();
                List<MetadataProto.Path> compactPaths = new ArrayList<>();
                List<MetadataProto.Path> projectionPaths = new ArrayList<>();
                this.splitPaths(pathDao.getAllByLayoutId(layoutId), orderedPaths, compactPaths, projectionPaths);
                MetadataProto.Layout layout = MetadataProto.Layout.newBuilder()
                        .setId(layoutId)
                        .setVersion(rs.getLong("LAYOUT_VERSION"))
                        .setPermission(convertPermission(rs.getShort("LAYOUT_PERMISSION")))
                        .setCreateAt(rs.getLong("LAYOUT_CREATE_AT"))
                        .setOrdered(rs.getString("LAYOUT_ORDERED"))
                        .addAllOrderedPaths(orderedPaths)
                        .setCompact(rs.getString("LAYOUT_COMPACT"))
                        .addAllCompactPaths(compactPaths)
                        .setSplits(rs.getString("LAYOUT_SPLITS"))
                        .setProjections(rs.getString("LAYOUT_PROJECTIONS"))
                        .addAllProjectionPaths(projectionPaths)
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID"))
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

    public long insert (MetadataProto.Layout layout)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO LAYOUTS(" +
                "`LAYOUT_VERSION`," +
                "`LAYOUT_CREATE_AT`," +
                "`LAYOUT_PERMISSION`," +
                "`LAYOUT_ORDERED`," +
                "`LAYOUT_COMPACT`," +
                "`LAYOUT_SPLITS`," +
                "`LAYOUT_PROJECTIONS`," +
                "`SCHEMA_VERSIONS_SV_ID`," +
                "`TBLS_TBL_ID`) VALUES (?,?,?,?,?,?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, layout.getVersion());
            pst.setLong(2, layout.getCreateAt());
            pst.setShort(3, convertPermission(layout.getPermission()));
            pst.setString(4, layout.getOrdered());
            pst.setString(5, layout.getCompact());
            pst.setString(6, layout.getSplits());
            pst.setString(7, layout.getProjections());
            pst.setLong(8, layout.getSchemaVersionId());
            pst.setLong(9, layout.getTableId());
            if (pst.executeUpdate() == 1)
            {
                ResultSet rs = pst.executeQuery("SELECT LAST_INSERT_ID()");
                if (rs.next())
                {
                    return rs.getLong(1);
                }
                else
                {
                    return -1;
                }
            }
            else
            {
                return -1;
            }
        } catch (SQLException e)
        {
            log.error("insert in RdbLayoutDao", e);
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
                "`LAYOUT_ORDERED` = ?," +
                "`LAYOUT_COMPACT` = ?," +
                "`LAYOUT_SPLITS` = ?," +
                "`LAYOUT_PROJECTIONS` = ?\n" +
                "WHERE `LAYOUT_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, layout.getVersion());
            pst.setLong(2, layout.getCreateAt());
            pst.setShort(3, convertPermission(layout.getPermission()));
            pst.setString(4, layout.getOrdered());
            pst.setString(5, layout.getCompact());
            pst.setString(6, layout.getSplits());
            pst.setString(7, layout.getProjections());
            pst.setLong(8, layout.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbLayoutDao", e);
        }

        return false;
    }
}
