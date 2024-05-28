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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.RangeIndexDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2024-05-25
 */
public class RdbRangeIndexDao extends RangeIndexDao
{
    public RdbRangeIndexDao() {}

    private static final Logger log = LogManager.getLogger(RdbRangeIndexDao.class);

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.RangeIndex getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM RANGE_INDEXES WHERE RI_ID=" + id);
            if (rs.next())
            {
                MetadataProto.RangeIndex rangeIndex = MetadataProto.RangeIndex.newBuilder()
                        .setId(id)
                        .setIndexStruct(ByteString.copyFrom(rs.getBytes("RI_INDEX_STRUCT")))
                        .setKeyColumns(rs.getString("RI_KEY_COLUMNS"))
                        .setTableId(rs.getLong("TBLS_TBL_ID"))
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                return rangeIndex;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbRangeIndexDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.RangeIndex> getAllByTableId(long tableId)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM RANGE_INDEXES WHERE TBLS_TBL_ID=" + tableId);
            List<MetadataProto.RangeIndex> rangeIndexes = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.RangeIndex rangeIndex = MetadataProto.RangeIndex.newBuilder()
                        .setId(rs.getLong("RI_ID"))
                        .setIndexStruct(ByteString.copyFrom(rs.getBytes("RI_INDEX_STRUCT")))
                        .setKeyColumns(rs.getString("RI_KEY_COLUMNS"))
                        .setTableId(tableId)
                        .setSchemaVersionId(rs.getLong("SCHEMA_VERSIONS_SV_ID")).build();
                rangeIndexes.add(rangeIndex);
            }
            return rangeIndexes;
        } catch (SQLException e)
        {
            log.error("getAllByTableId in RdbRangeIndexDao", e);
        }

        return null;
    }

    @Override
    public MetadataProto.RangeIndex getByTableAndSvIds(long tableId, long schemaVersionId)
    {
        Connection conn = db.getConnection();
        String sql = "SELECT * FROM RANGE_INDEXES WHERE TBLS_TBL_ID=? AND SCHEMA_VERSIONS_SV_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, tableId);
            pst.setLong(2, schemaVersionId);
            ResultSet rs = pst.executeQuery();
            if (rs.next())
            {
                MetadataProto.RangeIndex rangeIndex = MetadataProto.RangeIndex.newBuilder()
                        .setId(rs.getLong("RI_ID"))
                        .setIndexStruct(ByteString.copyFrom(rs.getBytes("RI_INDEX_STRUCT")))
                        .setKeyColumns(rs.getString("RI_KEY_COLUMNS"))
                        .setTableId(tableId)
                        .setSchemaVersionId(schemaVersionId).build();
                return rangeIndex;
            }
        } catch (SQLException e)
        {
            log.error("getByTableAndSvIds in RdbRangeIndexDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.RangeIndex rangeIndex)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM RANGE_INDEXES WHERE RI_ID=" + rangeIndex.getId();
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbRangeIndexDao", e);
        }

        return false;
    }

    @Override
    public long insert(MetadataProto.RangeIndex rangeIndex)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO RANGE_INDEXES(" +
                "`RI_INDEX_STRUCT`," +
                "`RI_KEY_COLUMNS`," +
                "`TBLS_TBL_ID`," +
                "`SCHEMA_VERSIONS_SV_ID`) VALUES (?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setBytes(1, rangeIndex.getIndexStruct().toByteArray());
            pst.setString(2, rangeIndex.getKeyColumns());
            pst.setLong(3, rangeIndex.getTableId());
            pst.setLong(4, rangeIndex.getSchemaVersionId());
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
            log.error("insert in RdbRangeIndexDao", e);
        }

        return -1;
    }

    @Override
    public boolean update(MetadataProto.RangeIndex rangeIndex)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE RANGE_INDEXES\n" +
                "SET\n" +
                "`RI_INDEX_STRUCT` = ?," +
                "`RI_KEY_COLUMNS` = ?," +
                "`TBLS_TBL_ID` = ?," +
                "`SCHEMA_VERSION_SV_ID` = ?\n" +
                "WHERE `RI_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setBytes(1, rangeIndex.getIndexStruct().toByteArray());
            pst.setString(2, rangeIndex.getKeyColumns());
            pst.setLong(3, rangeIndex.getTableId());
            pst.setLong(4, rangeIndex.getSchemaVersionId());
            pst.setLong(5, rangeIndex.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbRangeIndexDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteById(long id)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM RANGE_INDEXES WHERE RI_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, id);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteById in RdbRangeIndexDao", e);
        }

        return false;
    }
}
