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

import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.utils.DBUtil;
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
    private static Logger log = LogManager.getLogger(RdbColumnDao.class);

    public RdbColumnDao() {}

    private static final DBUtil db = DBUtil.Instance();

    @Override
    public MetadataProto.Column getById(long id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_NAME, COL_TYPE, COL_SIZE, TBLS_TBL_ID FROM COLS WHERE COL_ID=" + id);
            if (rs.next())
            {
                MetadataProto.Column column = MetadataProto.Column.newBuilder()
                .setId(id)
                .setName(rs.getString("COL_NAME"))
                .setType(rs.getString("COL_TYPE"))
                .setSize(rs.getDouble("COL_SIZE"))
                .setTableId(rs.getLong("TBLS_TBL_ID")).build();
                return column;
            }

        } catch (SQLException e)
        {
            log.error("getById in RdbColumnDao", e);
        }

        return null;
    }

    public List<MetadataProto.Column> getByTable(MetadataProto.Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_ID, COL_NAME, COL_TYPE, COL_SIZE FROM COLS WHERE TBLS_TBL_ID=" + table.getId() +
                    " ORDER BY COL_ID");
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

        return null;
    }

    public Order getOrderByTable(MetadataProto.Table table)
    {
        Order columnOrder = new Order();
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
            columnOrder.setColumnOrder(columns);
            return columnOrder;

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
                "`COL_SIZE` = ?\n" +
                "WHERE `COL_ID` = ?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setString(1, column.getName());
            pst.setString(2, column.getType());
            pst.setDouble(3, column.getSize());
            pst.setLong(4, column.getId());

            return pst.executeUpdate() == 1;
        } catch (SQLException e)
        {
            log.error("getByTable in RdbColumnDao", e);
        }

        return false;
    }

    public int insertBatch (MetadataProto.Table table, List<MetadataProto.Column> columns)
    {
        StringBuilder sql = new StringBuilder("INSERT INTO COLS (COL_NAME,COL_TYPE,COL_SIZE,TBLS_TBL_ID)" +
                "VALUES ");
        for (MetadataProto.Column column : columns)
        {
            sql.append("('").append(column.getName()).append("','").append(column.getType())
                    .append("',").append(column.getSize()).append(",").append(table.getId()).append("),");
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
