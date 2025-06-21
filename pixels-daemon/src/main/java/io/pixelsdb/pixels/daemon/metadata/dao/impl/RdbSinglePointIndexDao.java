/*
 * Copyright 2025 PixelsDB.
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
import io.pixelsdb.pixels.daemon.metadata.dao.SinglePointIndexDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2025-02-07
 */
public class RdbSinglePointIndexDao extends SinglePointIndexDao
{
    public RdbSinglePointIndexDao() { }

    private static final Logger log = LogManager.getLogger(RdbSinglePointIndexDao.class);

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.SinglePointIndex getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM SINGLE_POINT_INDICES WHERE SPI_ID=" + id);
            if (rs.next())
            {
                MetadataProto.SinglePointIndex singlePointIndex = MetadataProto.SinglePointIndex.newBuilder()
                        .setId(id)
                        .setKeyColumns(rs.getString("SPI_KEY_COLUMNS"))
                        .setPrimary(rs.getBoolean("SPI_PRIMARY"))
                        .setUnique(rs.getBoolean("SPI_UNIQUE"))
                        .setIndexScheme(rs.getString("SPI_INDEX_SCHEME"))
                        .setTableId(rs.getLong("TBLS_TBL_ID"))
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                return singlePointIndex;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbSinglePointIndexDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.SinglePointIndex> getAllByTableId(long tableId)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM SINGLE_POINT_INDICES WHERE TBLS_TBL_ID=" + tableId);
            List<MetadataProto.SinglePointIndex> singlePointIndices = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.SinglePointIndex secondaryIndex = MetadataProto.SinglePointIndex.newBuilder()
                        .setId(rs.getLong("SPI_ID"))
                        .setKeyColumns(rs.getString("SPI_KEY_COLUMNS"))
                        .setPrimary(rs.getBoolean("SPI_PRIMARY"))
                        .setUnique(rs.getBoolean("SPI_UNIQUE"))
                        .setIndexScheme(rs.getString("SPI_INDEX_SCHEME"))
                        .setTableId(tableId)
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                singlePointIndices.add(secondaryIndex);
            }
            return singlePointIndices;
        } catch (SQLException e)
        {
            log.error("getAllByTableId in RdbSinglePointIndexDao", e);
        }

        return null;
    }

    @Override
    public MetadataProto.SinglePointIndex getByTableId(long tableId)
    {
        Connection conn = db.getConnection();
        String sql = "SELECT * FROM SINGLE_POINT_INDICES WHERE TBLS_TBL_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, tableId);
            ResultSet rs = pst.executeQuery();
            if (rs.next())
            {
                MetadataProto.SinglePointIndex singlePointIndex = MetadataProto.SinglePointIndex.newBuilder()
                        .setId(rs.getLong("SPI_ID"))
                        .setKeyColumns(rs.getString("SPI_KEY_COLUMNS"))
                        .setPrimary(rs.getBoolean("SPI_PRIMARY"))
                        .setUnique(rs.getBoolean("SPI_UNIQUE"))
                        .setIndexScheme(rs.getString("SPI_INDEX_SCHEME"))
                        .setTableId(tableId)
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                return singlePointIndex;
            }
        } catch (SQLException e)
        {
            log.error("getByTableAndSvIds in RdbSinglePointIndexDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.SinglePointIndex secondaryIndex)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM SINGLE_POINT_INDICES WHERE SPI_ID=" + secondaryIndex.getId();
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbSinglePointIndexDao", e);
        }

        return false;
    }

    @Override
    public long insert(MetadataProto.SinglePointIndex singlePointIndex)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO SINGLE_POINT_INDICES(" +
                "`SPI_KEY_COLUMNS`," +
                "`SPI_PRIMARY`," +
                "`SPI_UNIQUE`," +
                "`SPI_INDEX_SCHEME`," +
                "`TBLS_TBL_ID`," +
                "`SCHEMA_VERSIONS_SV_ID`) VALUES (?,?,?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, singlePointIndex.getKeyColumns());
            pst.setBoolean(2, singlePointIndex.getPrimary());
            pst.setBoolean(3, singlePointIndex.getUnique());
            pst.setString(4, singlePointIndex.getIndexScheme());
            pst.setLong(5, singlePointIndex.getTableId());
            pst.setLong(6, singlePointIndex.getSchemaVersionId());
            if (pst.executeUpdate() == 1)
            {
                ResultSet rs = pst.executeQuery("SELECT LAST_INSERT_ID()");
                if (rs.next())
                {
                    return rs.getLong(1);
                } else
                {
                    return -1;
                }
            } else
            {
                return -1;
            }
        } catch (SQLException e)
        {
            log.error("insert in RdbSinglePointIndexDao", e);
        }

        return -1;
    }

    @Override
    public boolean update(MetadataProto.SinglePointIndex singlePointIndex)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE SINGLE_POINT_INDICES\n" +
                "SET\n" +
                "`SPI_KEY_COLUMNS` = ?," +
                "`SPI_PRIMARY` = ?," +
                "`SPI_UNIQUE` = ?," +
                "`SPI_INDEX_SCHEME` = ?," +
                "`TBLS_TBL_ID` = ?," +
                "`SCHEMA_VERSIONS_SV_ID` = ?\n" +
                "WHERE `SPI_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, singlePointIndex.getKeyColumns());
            pst.setBoolean(2, singlePointIndex.getPrimary());
            pst.setBoolean(3, singlePointIndex.getUnique());
            pst.setString(4, singlePointIndex.getIndexScheme());
            pst.setLong(5, singlePointIndex.getTableId());
            pst.setLong(6, singlePointIndex.getSchemaVersionId());
            pst.setLong(7, singlePointIndex.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbSinglePointIndexDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteByTableId(long tableId)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM SINGLE_POINT_INDICES WHERE TBLS_TBL_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, tableId);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteById in RdbSinglePointIndexDao", e);
        }

        return false;
    }
}
