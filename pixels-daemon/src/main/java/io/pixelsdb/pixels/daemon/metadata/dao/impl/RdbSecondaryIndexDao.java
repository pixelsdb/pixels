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
import io.pixelsdb.pixels.daemon.metadata.dao.SecondaryIndexDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2025-02-07
 */
public class RdbSecondaryIndexDao extends SecondaryIndexDao
{
    public RdbSecondaryIndexDao()
    {
    }

    private static final Logger log = LogManager.getLogger(RdbSecondaryIndexDao.class);

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.SecondaryIndex getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM SECONDARY_INDEXES WHERE SI_ID=" + id);
            if (rs.next())
            {
                MetadataProto.SecondaryIndex secondaryIndex = MetadataProto.SecondaryIndex.newBuilder()
                        .setId(id)
                        .setKeyColumns(rs.getString("SI_KEY_COLUMNS"))
                        .setUnique(rs.getBoolean("SI_UNIQUE"))
                        .setIndexScheme(rs.getString("SI_INDEX_SCHEME"))
                        .setTableId(rs.getLong("TBLS_TBL_ID"))
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                return secondaryIndex;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbSecondaryIndexDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.SecondaryIndex> getAllByTableId(long tableId)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM SECONDARY_INDEXES WHERE TBLS_TBL_ID=" + tableId);
            List<MetadataProto.SecondaryIndex> secondaryIndexes = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.SecondaryIndex secondaryIndex = MetadataProto.SecondaryIndex.newBuilder()
                        .setId(rs.getLong("SI_ID"))
                        .setKeyColumns(rs.getString("SI_KEY_COLUMNS"))
                        .setUnique(rs.getBoolean("SI_UNIQUE"))
                        .setIndexScheme(rs.getString("SI_INDEX_SCHEME"))
                        .setTableId(tableId)
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                secondaryIndexes.add(secondaryIndex);
            }
            return secondaryIndexes;
        } catch (SQLException e)
        {
            log.error("getAllByTableId in RdbSecondaryIndexDao", e);
        }

        return null;
    }

    @Override
    public MetadataProto.SecondaryIndex getByTableId(long tableId)
    {
        Connection conn = db.getConnection();
        String sql = "SELECT * FROM SECONDARY_INDEXES WHERE TBLS_TBL_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, tableId);
            ResultSet rs = pst.executeQuery();
            if (rs.next())
            {
                MetadataProto.SecondaryIndex secondaryIndex = MetadataProto.SecondaryIndex.newBuilder()
                        .setId(rs.getLong("SI_ID"))
                        .setKeyColumns(rs.getString("SI_KEY_COLUMNS"))
                        .setUnique(rs.getBoolean("SI_UNIQUE"))
                        .setIndexScheme(rs.getString("SI_INDEX_SCHEME"))
                        .setTableId(tableId)
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                return secondaryIndex;
            }
        } catch (SQLException e)
        {
            log.error("getByTableAndSvIds in RdbSecondaryIndexDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.SecondaryIndex secondaryIndex)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM SECONDARY_INDEXES WHERE SI_ID=" + secondaryIndex.getId();
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbSecondaryIndexDao", e);
        }

        return false;
    }

    @Override
    public long insert(MetadataProto.SecondaryIndex secondaryIndex)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO SECONDARY_INDEXES(" +
                "`SI_KEY_COLUMNS`," +
                "`SI_UNIQUE`," +
                "`SI_INDEX_SCHEME`," +
                "`TBLS_TBL_ID`," +
                "`SCHEMA_VERSIONS_SV_ID`) VALUES (?,?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, secondaryIndex.getKeyColumns());
            pst.setBoolean(2, secondaryIndex.getUnique());
            pst.setString(3, secondaryIndex.getIndexScheme());
            pst.setLong(4, secondaryIndex.getTableId());
            pst.setLong(5, secondaryIndex.getSchemaVersionId());
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
            log.error("insert in RdbSecondaryIndexDao", e);
        }

        return -1;
    }

    @Override
    public boolean update(MetadataProto.SecondaryIndex secondaryIndex)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE SECONDARY_INDEXES\n" +
                "SET\n" +
                "`RI_KEY_COLUMNS` = ?," +
                "`SI_UNIQUE` = ?," +
                "`RI_INDEX_SCHEME` = ?," +
                "`TBLS_TBL_ID` = ?," +
                "`SCHEMA_VERSION_SV_ID` = ?\n" +
                "WHERE `RI_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, secondaryIndex.getKeyColumns());
            pst.setBoolean(2, secondaryIndex.getUnique());
            pst.setString(3, secondaryIndex.getIndexScheme());
            pst.setLong(4, secondaryIndex.getTableId());
            pst.setLong(5, secondaryIndex.getSchemaVersionId());
            pst.setLong(6, secondaryIndex.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbSecondaryIndexDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteByTableId(long tableId)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM SECONDARY_INDEXES WHERE TBLS_TBL_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, tableId);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteById in RdbSecondaryIndexDao", e);
        }

        return false;
    }
}
