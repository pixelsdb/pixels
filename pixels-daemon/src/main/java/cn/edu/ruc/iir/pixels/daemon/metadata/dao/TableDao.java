package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.dao
 * @ClassName: TableDao
 * @Description: TblsDao
 * @author: tao
 * @date: Create in 2018-01-26 10:37
 **/
public class TableDao implements BaseDao<Table> {

    @Override
    public Table get(Serializable id) {
        return null;
    }

    @Override
    public List<Table> find(String sql) {
        return null;
    }

    @Override
    public List<Table> loadAll(String sql, String[] params) {
        Connection conn = db.getConnection();
        PreparedStatement psmt = null;
        ResultSet rs = null;
        List<Table> l = new ArrayList<Table>();
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
                Table s = new Table();
                s.setTblId(rs.getInt("TBL_ID"));
                s.setTblName(rs.getString("TBL_NAME"));
                s.setTblType(rs.getString("TBL_TYPE"));
                l.add(s);
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
        return l;
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
    public List<Table> find(Table o) {
        return null;
    }

    @Override
    public List<Table> loadAll(Table o) {
        return null;
    }

    @Override
    public boolean update(Table o, String[] params) {
        return false;
    }

    public int getDbIdbyDbName(String dbName) {
        int res = 0;
        String sql = "SELECT TBL_ID from TBLS where TBL_NAME = ?";
        Connection conn = db.getConnection();
        PreparedStatement psmt = null;
        ResultSet rs = null;
        try {
            psmt = conn.prepareStatement(sql);
            psmt.setString(1, dbName);
            rs = psmt.executeQuery();
            while (rs.next()) {
                res = rs.getInt("TBL_ID");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (psmt != null)
                    psmt.close();
                if (conn != null)
                    conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }
}
