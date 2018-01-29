package cn.edu.ruc.iir.pixels.metadata.util;

import cn.edu.ruc.iir.pixels.common.DBUtils;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.common
 * @ClassName: TestDBUtil
 * @Description: test dbutil
 * @author: tao
 * @date: Create in 2018-01-25 10:02
 **/
public class TestDBUtil {


    @Test
    public void testDBUtils() throws SQLException {
        String sql = "select * from DBS";
        DBUtils db = DBUtils.Instance();
        ResultSet rs = db.getQuery(sql, null);
        int n = rs.getRow();
        System.out.println(n);

//        sql = "insert into DBS(DB_NAME, DB_DESC) values(?, ?)";
//        db.getUpdate(sql, new String[]{"Pixels", "pixel test schema"});

        sql = "select count(*) from DBS";
        rs = db.getQuery(sql);
        if(rs.next())
            n = rs.getInt(1);
        System.out.println(n);

        sql = "select count(*) from DBS where DB_ID = ?";
        rs = db.getQuery(sql, new String[]{"2"});
        n = rs.getRow();
        System.out.println(n);
    }


}
