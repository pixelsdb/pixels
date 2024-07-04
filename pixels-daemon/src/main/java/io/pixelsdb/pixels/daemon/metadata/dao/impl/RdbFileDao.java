/*
 * Copyright 2024 PixelsDB.
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
import io.pixelsdb.pixels.daemon.metadata.dao.FileDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2024-06-08
 */
public class RdbFileDao extends FileDao
{
    public RdbFileDao() { }

    private static final Logger log = LogManager.getLogger(RdbPathDao.class);

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.File getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM FILES WHERE FILE_ID=" + id);
            if (rs.next())
            {
                return MetadataProto.File.newBuilder().setId(id)
                        .setName(rs.getString("FILE_NAME"))
                        .setNumRowGroup(rs.getInt("FILE_NUM_RG"))
                        .setPathId(rs.getLong("PATHS_PATH_ID")).build();
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbFileDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.File> getAllByPathId(long pathId)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM FILES WHERE PATHS_PATH_ID=" + pathId);
            List<MetadataProto.File> files = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.File.Builder builder = MetadataProto.File.newBuilder()
                        .setId(rs.getLong("FILE_ID"))
                        .setName(rs.getString("FILE_NAME"))
                        .setNumRowGroup(rs.getInt("FILE_NUM_RG"))
                        .setPathId(rs.getLong("PATHS_PATH_ID"));
                files.add(builder.build());
            }
            return files;
        } catch (SQLException e)
        {
            log.error("getAllByPathId in RdbFileDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.File file)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM FILES WHERE FILE_ID=" + file.getId();
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbFileDao", e);
        }

        return false;
    }

    @Override
    public long insert(MetadataProto.File file)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO FILES(" +
                "`FILE_NAME`," +
                "`FILE_NUM_RG`," +
                "`PATHS_PATH_ID`) VALUES (?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, file.getName());
            pst.setInt(2, file.getNumRowGroup());
            pst.setLong(3, file.getPathId());
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
            log.error("insert in RdbFileDao", e);
        }

        return -1;
    }

    @Override
    public boolean insertBatch(List<MetadataProto.File> files)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO FILES(" +
                "`FILE_NAME`," +
                "`FILE_NUM_RG`," +
                "`PATHS_PATH_ID`) VALUES (?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            for (MetadataProto.File file : files)
            {
                pst.setString(1, file.getName());
                pst.setInt(2, file.getNumRowGroup());
                pst.setLong(3, file.getPathId());
                pst.addBatch();
            }
            pst.executeBatch();
            return true;
        } catch (SQLException e)
        {
            log.error("insertBatch in RdbFileDao", e);
        }
        return false;
    }

    @Override
    public boolean update(MetadataProto.File file)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE FILES\n" +
                "SET\n" +
                "`FILE_NAME` = ?," +
                "`FILE_NUM_RG` = ?\n" +
                "WHERE `FILE_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, file.getName());
            pst.setInt(2, file.getNumRowGroup());
            pst.setLong(3, file.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbFileDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteByIds(List<Long> ids)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM FILES WHERE FILE_ID=?";
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
            log.error("deleteById in RdbFileDao", e);
        }

        return false;
    }
}
