package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;

import java.io.Serializable;
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
        ResultSet rs = db.getQuery(sql, params);
        List<Table> l = new ArrayList<Table>();
        try {
            while (rs.next()) {
                Table s = new Table();
                s.setTblId(rs.getInt("TBL_ID"));
                s.setTblName(rs.getString("TBL_NAME"));
                s.setTblType(rs.getString("TBL_TYPE"));
                l.add(s);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return l;
    }

    @Override
    public boolean update(String sql, String[] params) {
        return false;
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
}
