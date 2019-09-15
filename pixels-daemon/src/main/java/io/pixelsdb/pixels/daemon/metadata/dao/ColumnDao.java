package io.pixelsdb.pixels.daemon.metadata.dao;


import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.utils.DBUtil;
import io.pixelsdb.pixels.daemon.MetadataProto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ColumnDao implements Dao<MetadataProto.Column>
{
    private static Logger log = LogManager.getLogger(ColumnDao.class);

    public ColumnDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static final TableDao tableDao = new TableDao();

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
            log.error("getById in ColumnDao", e);
        }

        return null;
    }

    @Override
    public List<MetadataProto.Column> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
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
            log.error("getByTable in ColumnDao", e);
        }

        return null;
    }

    @SuppressWarnings("Duplicates")
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
            log.error("getByTable in ColumnDao", e);
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

            return pst.execute();
        } catch (SQLException e)
        {
            log.error("getByTable in ColumnDao", e);
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
            log.error("insertBatch in ColumnDao", e);
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
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("delete in ColumnDao", e);
        }
        return false;
    }
}
