package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Layout;

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
 * @ClassName: LayoutDao
 * @Description: LayoutsDao
 * @author: tao
 * @date: Create in 2018-01-26 10:36
 **/
public class LayoutDao implements BaseDao<Layout> {

    @Override
    public Layout get(Serializable id) {
        return null;
    }

    @Override
    public List<Layout> find(String sql) {
        return null;
    }

    @Override
    public List<Layout> loadAll(String sql, String[] params) {
        Connection conn = db.getConnection();
        PreparedStatement psmt = null;
        ResultSet rs = null;
        List<Layout> layoutList = new ArrayList<Layout>();
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
                Layout layout = new Layout();
                layout.setLayInitPath(rs.getString("LAYOUT_INIT_PATH"));
                layout.setLaySplit(rs.getString("LAYOUT_SPLIT"));
                layoutList.add(layout);
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
        return layoutList;
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
                if (conn != null)
                    conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result == 1;
    }

    @Override
    public List<Layout> find(Layout o) {
        return null;
    }

    @Override
    public List<Layout> loadAll(Layout o) {
        return null;
    }

    @Override
    public boolean update(Layout o, String[] params) {
        return false;
    }
}
