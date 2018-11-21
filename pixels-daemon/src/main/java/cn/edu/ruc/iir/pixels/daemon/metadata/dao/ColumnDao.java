package cn.edu.ruc.iir.pixels.daemon.metadata.dao;


import cn.edu.ruc.iir.pixels.common.metadata.MetadataClientHandler;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Order;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ColumnDao implements Dao<Column>
{
    private static Logger log = LogManager.getLogger(MetadataClientHandler.class);

    public ColumnDao() {}

    private static final DBUtil db = DBUtil.Instance();
    private static final TableDao tableModel = new TableDao();

    @Override
    public Column getById(int id)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_NAME, COL_TYPE, COL_SIZE, TBLS_TBL_ID FROM COLS WHERE COL_ID=" + id);
            if (rs.next())
            {
                Column column = new Column();
                column.setId(id);
                column.setName(rs.getString("COL_NAME"));
                column.setType(rs.getString("COL_TYPE"));
                column.setSize(rs.getDouble("COL_SIZE"));
                column.setTable(tableModel.getById(rs.getInt("TBLS_TBL_ID")));
                return column;
            }

        } catch (SQLException e)
        {
            log.error("getById in ColumnDao", e);
        }

        return null;
    }

    @Override
    public List<Column> getAll()
    {
        throw new UnsupportedOperationException("getAll is not supported.");
    }

    public List<Column> getByTable(Table table)
    {
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_ID, COL_NAME, COL_TYPE, COL_SIZE FROM COLS WHERE TBLS_TBL_ID=" + table.getId() +
                    " ORDER BY COL_ID");
            List<Column> columns = new ArrayList<>();
            while (rs.next())
            {
                Column column = new Column();
                column.setId(rs.getInt("COL_ID"));
                column.setName(rs.getString("COL_NAME"));
                column.setType(rs.getString("COL_TYPE"));
                column.setSize(rs.getDouble("COL_SIZE"));
                column.setTable(table);
                table.addColumn(column);
                columns.add(column);
            }
            return columns;

        } catch (SQLException e)
        {
            log.error("getByTable in ColumnDao", e);
        }

        return null;
    }

    public Order getOrderByTable(Table table)
    {
        Order columnOrder = new Order();
        Connection conn = db.getConnection();
        try (Statement st = conn.createStatement())
        {
            ResultSet rs = st.executeQuery("SELECT COL_NAME FROM COLS WHERE TBLS_TBL_ID=" + table.getId() +
                    " ORDER BY COL_ID");
            List<String> column = new ArrayList<>();
            String colName = null;
            while (rs.next())
            {
                colName = rs.getString("COL_NAME");
                column.add(colName);
            }
            columnOrder.setColumnOrder(column);
            return columnOrder;

        } catch (SQLException e)
        {
            log.error("getByTable in ColumnDao", e);
        }

        return null;
    }

    public boolean update(Column column)
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
            pst.setInt(4, column.getId());

            return pst.execute();
        } catch (SQLException e)
        {
            log.error("getByTable in ColumnDao", e);
        }

        return false;
    }

    public int insertBatch (Table table, List<Column> columns)
    {
        StringBuilder sql = new StringBuilder("INSERT INTO COLS (COL_NAME,COL_TYPE,COL_SIZE,TBLS_TBL_ID)" +
                "VALUES ");
        for (Column column : columns)
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

    public boolean deleteByTable (Table table)
    {
        Connection conn = db.getConnection();
        String sql = "DELETE FROM COLS WHERE TBLS_TBL_ID=?";
        try (PreparedStatement pst = conn.prepareStatement(sql))
        {
            pst.setInt(1, table.getId());
            return pst.execute();
        } catch (SQLException e)
        {
            log.error("delete in ColumnDao", e);
        }
        return false;
    }
}
