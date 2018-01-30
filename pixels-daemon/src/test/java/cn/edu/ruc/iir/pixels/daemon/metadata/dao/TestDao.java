package cn.edu.ruc.iir.pixels.daemon.metadata.dao;

import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Catalog;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.util.dao
 * @ClassName: TestDao
 * @Description: test dao
 * @author: tao
 * @date: Create in 2018-01-26 14:31
 **/
public class TestDao {


    @Test
    public void testDao() throws SQLException {
        BaseDao baseDao = new SchemaDao();
        String sql = "select * from DBS";
        List<Schema> schemas = baseDao.loadAll(sql);
        for (Schema s : schemas) {
            System.out.println(s.getScheId() + "\t" + s.getSchName() + "\t" + s.getSchDesc());
        }
    }

    @Test
    public void testTblDao() throws SQLException {
        String params[] = new String[]{"default"};
//        String params[] = new String[]{};
        BaseDao baseDao = new TableDao();
        String sql = "select * from TBLS " + (params.length > 0 ? "where DBS_DB_ID in (select DB_ID from DBS where DB_NAME = ? )" : "");
        List<Table> t = baseDao.loadAll(sql, params);
        for (Table s : t) {
            System.out.println(s.toString());
        }
    }

    @Test
    public void testTblLoadDao() throws SQLException {
        BaseDao baseDao = new SchemaDao();
        String sql = "select * from TBLS";
        List<Table> t = baseDao.loadAll(sql);
        for (Table s : t) {
            System.out.println(s.toString());
        }
    }

    @Test
    public void testTblUpdateDao() throws SQLException {
        BaseDao baseDao = new SchemaDao();
        String sql = "insert into TBLS(TBL_NAME,TBL_TYPE,DBS_DB_ID) values(?,?,?)";
        String[] params = new String[]{"demo2", "USER", "2"};
        boolean bool = baseDao.update(sql, params);
        System.out.println(bool);
    }

    @Test
    public void testCatalogDao() throws SQLException {
        String params[] = new String[]{"test"};
        BaseDao baseDao = new CatalogDao();
        String sql = "select * from CATALOG " + (params.length > 0 ? "where LAYOUTS_LAYOUT_ID in (select LAYOUT_ID from LAYOUTS where TBLS_TBL_ID in (select TBL_ID from TBLS where TBL_NAME = ? ) )" : "");
        List<Catalog> t = baseDao.loadAll(sql, params);
        for (Catalog c : t) {
            System.out.println(c.toString());
        }
    }
}
