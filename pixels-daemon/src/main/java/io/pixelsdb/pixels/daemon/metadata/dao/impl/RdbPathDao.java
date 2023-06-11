/*
 * Copyright 2023 PixelsDB.
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
import io.pixelsdb.pixels.daemon.metadata.dao.PathDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2023-06-10
 */
public class RdbPathDao extends PathDao
{
    public RdbPathDao() {}

    private static final Logger log = LogManager.getLogger(RdbPathDao.class);

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.Path getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PATHS WHERE PATH_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Path path = MetadataProto.Path.newBuilder()
                        .setId(id)
                        .setUri(rs.getString("PATH_URI"))
                        .setIsCompact(rs.getBoolean("PATH_IS_COMPACT"))
                        .setLayoutId(rs.getLong("LAYOUTS_LAYOUT_ID"))
                        // Issue #437: range id is set to 0 if it is null in metadata.
                        .setRangeId(rs.getLong("RANGES_RANGE_ID")).build();
                return path;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbPathDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.Path> getAllByLayoutId(long layoutId)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PATHS WHERE LAYOUTS_LAYOUT_ID=" + layoutId);
            List<MetadataProto.Path> paths = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Path path = MetadataProto.Path.newBuilder()
                        .setId(rs.getLong("PATH_ID"))
                        .setUri(rs.getString("PATH_URI"))
                        .setIsCompact(rs.getBoolean("PATH_IS_COMPACT"))
                        .setLayoutId(layoutId)
                        // Issue #437: range id is set to 0 if it is null in metadata.
                        .setRangeId(rs.getLong("RANGES_RANGE_ID")).build();
                paths.add(path);
            }
            return paths;
        } catch (SQLException e)
        {
            log.error("getAllByLayoutId in RdbPathDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.Path> getAllByRangeId(long rangeId)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM PATHS WHERE RANGES_RANGE_ID=" + rangeId);
            List<MetadataProto.Path> paths = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Path path = MetadataProto.Path.newBuilder()
                        .setId(rs.getLong("PATH_ID"))
                        .setUri(rs.getString("PATH_URI"))
                        .setIsCompact(rs.getBoolean("PATH_IS_COMPACT"))
                        .setLayoutId(rs.getLong("LAYOUTS_LAYOUT_ID"))
                        .setRangeId(rangeId).build();
                paths.add(path);
            }
            return paths;
        } catch (SQLException e)
        {
            log.error("getAllByRange in RdbPathDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.Path path)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM PATHS WHERE PATH_ID=" + path.getId();
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbPathDao", e);
        }

        return false;
    }

    @Override
    public boolean insert(MetadataProto.Path path)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO PATHS(" +
                "`PATH_URI`," +
                "`PATH_IS_COMPACT`," +
                "`LAYOUTS_LAYOUT_ID`," +
                "`RANGES_RANGE_ID`) VALUES (?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, path.getUri());
            pst.setBoolean(2, path.getIsCompact());
            pst.setLong(3, path.getLayoutId());
            if (path.hasRangeId())
            {
                pst.setLong(4, path.getRangeId());
            }
            else
            {
                pst.setNull(4, Types.BIGINT);
            }
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("insert in RdbPathDao", e);
        }

        return false;
    }

    @Override
    public boolean update(MetadataProto.Path path)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE PATHS\n" +
                "SET\n" +
                "`PATH_URI` = ?," +
                "`PATH_IS_COMPACT` = ?\n" +
                "WHERE `PATH_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, path.getUri());
            pst.setBoolean(2, path.getIsCompact());
            pst.setLong(3, path.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbPathDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteByIds(List<Long> ids)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM PATHS WHERE PATH_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            for (Long id : ids)
            {
                pst.setLong(1, id);
                pst.addBatch();
            }
            pst.executeBatch();
            return true;
        } catch (SQLException e)
        {
            log.error("deleteById in RdbPathDao", e);
        }

        return false;
    }
}
