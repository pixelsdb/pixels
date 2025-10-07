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
import io.pixelsdb.pixels.daemon.metadata.dao.RangeDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 * @create 2024-05-25
 */
public class RdbRangeDao extends RangeDao
{
    private static final Logger log = LogManager.getLogger(RdbRangeDao.class);

    public RdbRangeDao() {}

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.Range getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM RANGES WHERE RANGE_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Range range = MetadataProto.Range.newBuilder()
                        .setId(id)
                        .setMin(ByteString.copyFrom(rs.getBytes("RANGE_MIN")))
                        .setMax(ByteString.copyFrom(rs.getBytes("RANGE_MAX")))
                        // Issue #658: parent id is set to 0 if it is null in metadata.
                        .setParentId(rs.getLong("RANGE_PARENT_ID"))
                        .setRangeIndexId(rs.getLong("RANGE_INDEXES_RI_ID")).build();
                return range;
            }
        } catch (SQLException e)
        {
            log.error("getById in RdbRangeDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.Range> getAllByRangeIndexId(long rangeIndexId)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT * FROM RANGES WHERE RANGE_INDEXES_RI_ID=" + rangeIndexId);
            List<MetadataProto.Range> ranges = new ArrayList<>();
            while (rs.next())
            {
                MetadataProto.Range range = MetadataProto.Range.newBuilder()
                        .setId(rs.getLong("RANGE_ID"))
                        .setMin(ByteString.copyFrom(rs.getBytes("RANGE_MIN")))
                        .setMax(ByteString.copyFrom(rs.getBytes("RANGE_MAX")))
                        // Issue #658: parent id is set to 0 if it is null in metadata.
                        .setParentId(rs.getLong("RANGE_PARENT_ID"))
                        .setRangeIndexId(rangeIndexId).build();
                ranges.add(range);
            }
            return ranges;
        } catch (SQLException e)
        {
            log.error("getAllByRangeIndexId in RdbRangeDao", e);
        }

        return null;
    }

    @Override
    public boolean exists(MetadataProto.Range range)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            String sql = "SELECT 1 FROM RANGES WHERE RANGE_ID=" + range.getId();
            ResultSet rs = st.executeQuery(sql);
            if (rs.next())
            {
                return true;
            }
        } catch (SQLException e)
        {
            log.error("exists in RdbRangeDao", e);
        }

        return false;
    }

    @Override
    public long insert(MetadataProto.Range range)
    {
        Connection conn = db.getConnection();
        String sql = "INSERT INTO RANGES(" +
                "`RANGE_MIN`," +
                "`RANGE_MAX`," +
                "`RANGE_PARENT_ID`," +
                "`RANGE_INDEXES_RI_ID`) VALUES (?,?,?,?)";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setBytes(1, range.getMin().toByteArray());
            pst.setBytes(2, range.getMax().toByteArray());
            if (range.hasParentId())
            {
                pst.setLong(3, range.getParentId());
            }
            else
            {
                pst.setNull(3, Types.BIGINT);
            }
            pst.setLong(4, range.getRangeIndexId());
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
            log.error("insert in RdbRangeDao", e);
        }

        return -1;
    }

    @Override
    public boolean update(MetadataProto.Range range)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE RANGES\n" +
                "SET\n" +
                "`RANGE_MIN` = ?," +
                "`RANGE_MXN` = ?," +
                "`RANGE_PARENT_ID` = ?," +
                "`RANGE_INDEXES_RI_ID` = ?\n" +
                "WHERE `RANGE_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setBytes(1, range.getMin().toByteArray());
            pst.setBytes(2, range.getMax().toByteArray());
            pst.setLong(3, range.getParentId());
            pst.setLong(4, range.getRangeIndexId());
            pst.setLong(5, range.getId());
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("update in RdbRangeDao", e);
        }

        return false;
    }

    @Override
    public boolean deleteById(long id)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM RANGES WHERE RANGE_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, id);
            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("deleteById in RdbRangeDao", e);
        }

        return false;
    }
}
