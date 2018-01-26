package cn.edu.ruc.iir.pixels.metadata.dao;

import cn.edu.ruc.iir.pixels.metadata.domain.Schema;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.dao
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
    public List<Schema> loadT(String sql) {
        ResultSet rs = db.getQuery(sql);
        List<Schema> l = new ArrayList<Schema>();
        try {
            while (rs.next()) {
                Schema s = new Schema();
                s.setScheId(rs.getInt("DB_ID"));
                s.setSchName(rs.getString("DB_NAME"));
                s.setSchDesc(rs.getString("DB_DESC"));
                l.add(s);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return l;
    }

    @Override
    public void update(String sql) {
        db.getUpdate(sql);
    }

    @Override
    public List<Schema> find(Schema o) {
        return null;
    }

    @Override
    public List<Schema> loadT(Schema o) {
        return null;
    }

    @Override
    public void update(Schema o) {

    }

}
