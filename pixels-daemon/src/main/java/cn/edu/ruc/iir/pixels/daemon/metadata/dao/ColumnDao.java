package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Layout;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
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
        ResultSet rs = db.getQuery(sql, params);
        List<Column> columnList = new ArrayList<Column>();
        try {
            while (rs.next()) {
                Column column = new Column();
                column.setColName(rs.getString("COL_NAME"));
                column.setColType(rs.getString("COL_TYPE"));
                columnList.add(column);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnList;
    }

    @Override
    public boolean update(String sql, String[] params) {
        return db.getUpdate(sql, params) == 1;
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
