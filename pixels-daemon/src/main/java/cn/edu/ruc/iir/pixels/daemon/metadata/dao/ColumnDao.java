package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.dao
 * @ClassName: ColumnDao
 * @Description: ColsDao
 * @author: tao
 * @date: Create in 2018-01-26 10:36
 **/
public class ColumnDao implements BaseDao<Column> {

    @Override
    public Column get(Serializable id) {
        return null;
    }

    @Override
    public List<Column> find(String sql) {
        return null;
    }

    @Override
    public List<Column> loadAll(String sql, String[] params) {
        Connection conn = db.getConnection();
        PreparedStatement psmt = null;
        ResultSet rs = null;
        List<Column> columnList = new ArrayList<Column>();
        try {
            psmt = conn.prepareStatement(sql);
            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    try {
                        psmt.setString(i + 1, params[i]);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
            rs = psmt.executeQuery();
            while (rs.next()) {
                Column column = new Column();
                column.setColId(rs.getInt("COL_ID"));
                column.setColName(rs.getString("COL_NAME"));
                column.setColType(rs.getString("COL_TYPE"));
                columnList.add(column);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (psmt != null)
                    psmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return columnList;
    }

    @Override
    public boolean update(String sql, String[] params) {
        Connection conn = db.getConnection();
        PreparedStatement psmt = null;
        int result = 0;
        try {
            psmt = conn.prepareStatement(sql);
            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    psmt.setString(i + 1, params[i]);
                }
            }
            result = psmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (psmt != null)
                    psmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result == 1;
    }

    @Override
    public List<Column> find(Column o) {
        return null;
    }

    @Override
    public List<Column> loadAll(Column o) {
        return null;
    }

    @Override
    public boolean update(Column o, String[] params) {
        return false;
    }
}
