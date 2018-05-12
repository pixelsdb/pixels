package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;

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
 * @ClassName: SchemaDao
 * @Description: DBsDao
 * @author: tao
 * @date: Create in 2018-01-26 10:36
 **/
public class SchemaDao implements BaseDao<Schema> {

    @Override
    public Schema get(Serializable id) {
        return null;
    }

    @Override
    public List<Schema> find(String sql) {
        return null;
    }

    @Override
    public List<Schema> loadAll(String sql, String[] params) {
        Connection conn = db.getConnection();
        PreparedStatement psmt = null;
        ResultSet rs = null;
        List<Schema> l = new ArrayList<Schema>();
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
                Schema s = new Schema();
                s.setScheId(rs.getInt("DB_ID"));
                s.setSchName(rs.getString("DB_NAME"));
                s.setSchDesc(rs.getString("DB_DESC"));
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
                if (conn != null)
                    conn.close();
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
    public List<Schema> find(Schema o) {
        return null;
    }

    @Override
    public List<Schema> loadAll(Schema o) {
        return null;
    }

    @Override
    public boolean update(Schema o, String[] params) {
        return false;
    }
}
