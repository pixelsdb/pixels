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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.domain.Ordered;
import io.pixelsdb.pixels.common.utils.MetaDBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.daemon.metadata.dao.ColumnDao;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hank
 */
public class RdbColumnDao extends ColumnDao
{
    private static final Logger log = LogManager.getLogger(RdbColumnDao.class);

    public RdbColumnDao() {}

    private static final MetaDBUtil db = MetaDBUtil.Instance();

    @Override
    public MetadataProto.Column getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery(
                    "SELECT COL_NAME, COL_TYPE, COL_CHUNK_SIZE, COL_SIZE, COL_NULL_FRACTION, " +
                            "COL_CARDINALITY, COL_RECORD_STATS, TBLS_TBL_ID " + "FROM COLS WHERE COL_ID=" + id);
            if (rs.next())
            {
                byte[] recordStats = rs.getBytes("COL_RECORD_STATS");
                MetadataProto.Column column = MetadataProto.Column.newBuilder()
                        .setId(id)
                        .setName(rs.getString("COL_NAME"))
                        .setType(rs.getString("COL_TYPE"))
                        .setChunkSize(rs.getDouble("COL_CHUNK_SIZE"))
                        .setSize(rs.getDouble("COL_SIZE"))
                        .setNullFraction(rs.getDouble("COL_NULL_FRACTION"))
                        .setCardinality(rs.getLong("COL_CARDINALITY"))
                        .setRecordStats(recordStats != null ?
                                ByteString.copyFrom(recordStats) : ByteString.EMPTY)
                        .setTableId(rs.getLong("TBLS_TBL_ID")).build();
                return column;
            }

        } catch (SQLException e)
        {
            log.error("getById in RdbColumnDao", e);
        }

        return null;
    }

    public List<MetadataProto.Column> getByTable(MetadataProto.Table table, boolean getStatistics)
    {
        Connection conn = db.getConnection();
        if (getStatistics)
        {
            try (Statement st = conn.createStatement())
            {
                ResultSet rs = st.executeQuery(
                        "SELECT COL_ID, COL_NAME, COL_TYPE, COL_CHUNK_SIZE, COL_SIZE, " +
                                "COL_NULL_FRACTION, COL_CARDINALITY, COL_RECORD_STATS " +
                                "FROM COLS WHERE TBLS_TBL_ID=" + table.getId() + " ORDER BY COL_ID");
                List<MetadataProto.Column> columns = new ArrayList<>();
                while (rs.next())
                {
                    byte[] recordStats = rs.getBytes("COL_RECORD_STATS");
                    MetadataProto.Column column = MetadataProto.Column.newBuilder()
                            .setId(rs.getLong("COL_ID"))
                            .setName(rs.getString("COL_NAME"))
                            .setType(rs.getString("COL_TYPE"))
                            .setChunkSize(rs.getDouble("COL_CHUNK_SIZE"))
                            .setSize(rs.getDouble("COL_SIZE"))
                            .setNullFraction(rs.getDouble("COL_NULL_FRACTION"))
                            .setCardinality(rs.getLong("COL_CARDINALITY"))
                            .setRecordStats(recordStats != null ?
                                    ByteString.copyFrom(recordStats) : ByteString.EMPTY)
                            .setTableId(table.getId()).build();
                    columns.add(column);
                }
                return columns;

            } catch (SQLException e)
            {
                log.error("getByTable in RdbColumnDao", e);
            }
        }
        else
        {
            try (Statement st = conn.createStatement())
            {
                ResultSet rs = st.executeQuery(
                        "SELECT COL_ID, COL_NAME, COL_TYPE, COL_SIZE FROM COLS WHERE TBLS_TBL_ID=" +
                                table.getId() + " ORDER BY COL_ID");
                List<MetadataProto.Column> columns = new ArrayList<>();
                while (rs.next())
                {
                    MetadataProto.Column column = MetadataProto.Column.newBuilder()
                            .setId(rs.getLong("COL_ID"))
                            .setName(rs.getString("COL_NAME"))
                            .setType(rs.getString("COL_TYPE"))
                            .setSize(rs.getDouble("COL_SIZE"))
                            .setTableId(table.getId()).build();
                    columns.add(column);
                }
                return columns;

            } catch (SQLException e)
            {
                log.error("getByTable in RdbColumnDao", e);
            }
        }

        return null;
    }

    public Ordered getOrderByTable(MetadataProto.Table table)
    {
        Ordered columnOrdered = new Ordered();
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_NAME FROM COLS WHERE TBLS_TBL_ID=" + table.getId() +
                    " ORDER BY COL_ID");
            List<String> columns = new ArrayList<>();
            String colName = null;
            while (rs.next())
            {
                colName = rs.getString("COL_NAME");
                columns.add(colName);
            }
            columnOrdered.setColumnOrder(columns);
            return columnOrdered;

        } catch (SQLException e)
        {
            log.error("getByTable in RdbColumnDao", e);
        }

        return null;
    }

    public boolean update(MetadataProto.Column column)
    {
        Connection conn = db.getConnection();
        String sql = "UPDATE COLS\n" +
                "SET\n" +
                "`COL_NAME` = ?," +
                "`COL_TYPE` = ?," +
                "`COL_CHUNK_SIZE` = ?," +
                "`COL_SIZE` = ?," +
                "`COL_NULL_FRACTION` = ?," +
                "`COL_CARDINALITY` = ?," +
                "`COL_RECORD_STATS` = ?\n" +
                "WHERE `COL_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, column.getName());
            pst.setString(2, column.getType());
            pst.setDouble(3, column.getChunkSize());
            pst.setDouble(4, column.getSize());
            pst.setDouble(5, column.getNullFraction());
            pst.setLong(6, column.getCardinality());
            pst.setBytes(7, column.getRecordStats().toByteArray());
            pst.setLong(8, column.getId());

            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("getByTable in RdbColumnDao", e);
        }

        return false;
    }

    public int insertBatch (MetadataProto.Table table, List<MetadataProto.Column> columns)
    {
        StringBuilder sql = new StringBuilder("INSERT INTO COLS (COL_NAME,COL_TYPE,TBLS_TBL_ID)" +
                "VALUES ");
        for (MetadataProto.Column column : columns)
        {
            sql.append("('").append(column.getName()).append("','").append(column.getType())
                    .append("',").append(table.getId()).append("),");
        }
        sql.deleteCharAt(sql.length()-1);
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            return st.executeUpdate(sql.toString());
        } catch (SQLException e)
        {
            log.error("insertBatch in RdbColumnDao", e);
        }
        return 0;
    }

    public boolean deleteByTable (MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM COLS WHERE TBLS_TBL_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setLong(1, table.getId());
            return pst.executeUpdate() > 0;
        } catch (SQLException e)
        {
            log.error("delete in RdbColumnDao", e);
        }
        return false;
    }
}
