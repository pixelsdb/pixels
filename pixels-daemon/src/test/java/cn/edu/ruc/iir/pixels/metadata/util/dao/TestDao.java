package cn.edu.ruc.iir.pixels.metadata.util.dao;

import cn.edu.ruc.iir.pixels.metadata.dao.BaseDao;
import cn.edu.ruc.iir.pixels.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.metadata.util.DBUtils;
import org.junit.Test;

import java.sql.ResultSet;
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

}
