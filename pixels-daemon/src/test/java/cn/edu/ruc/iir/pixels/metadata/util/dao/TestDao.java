package cn.edu.ruc.iir.pixels.metadata.util.dao;

import cn.edu.ruc.iir.pixels.metadata.dao.BaseDao;
import cn.edu.ruc.iir.pixels.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.metadata.domain.Table;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.util.dao
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
        List<Schema> schemas = baseDao.loadT(sql);
        for (Schema s : schemas) {
            System.out.println(s.getScheId() + "\t" + s.getSchName() + "\t" + s.getSchDesc());
        }
    }

    @Test
    public void testTblLoadDao() throws SQLException {
        BaseDao baseDao = new SchemaDao();
        String sql = "select * from TBLS";
        List<Table> t = baseDao.loadT(sql);
        for (Table s : t) {
            System.out.println(s.toString());
        }
    }

    @Test
    public void testTblUpdateDao() throws SQLException {
        BaseDao baseDao = new SchemaDao();
        String sql = "insert into TBLS(TBL_NAME,TBL_TYPE,DBS_DB_ID) values(?,?,?)";
        String[] params = new String[]{"test", "USER", "4"};
        boolean bool = baseDao.update(sql, params);
        System.out.println(bool);
    }

}
