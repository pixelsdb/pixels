package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Catalog;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.dao
 * @ClassName: CatalogDao
 * @Description: CatalogsDao
 * @author: tao
 * @date: Create in 2018-01-26 10:37
 **/
public class CatalogDao implements BaseDao<Catalog> {
    @Override
    public Catalog get(Serializable id) {
        return null;
    }

    @Override
    public List<Catalog> find(String sql) {
        return null;
    }

    @Override
    public List<Catalog> loadAll(String sql, String[] params) {
        ResultSet rs = db.getQuery(sql, params);
        List<Catalog> l = new ArrayList<Catalog>();
        try {
            while (rs.next()) {
                Catalog c = new Catalog();
                c.setCataId(rs.getInt("CATALOG_ID"));
                c.setCataPath(rs.getString("CATALOG_PATH"));
                l.add(c);
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
    public List<Catalog> find(Catalog o) {
        return null;
    }

    @Override
    public List<Catalog> loadAll(Catalog o) {
        return null;
    }

    @Override
    public boolean update(Catalog o, String[] params) {
        return false;
    }
}
