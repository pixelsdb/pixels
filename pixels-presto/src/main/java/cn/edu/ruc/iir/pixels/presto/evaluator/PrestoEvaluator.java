package cn.edu.ruc.iir.pixels.presto.evaluator;

import java.sql.*;
import java.util.Properties;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.evaluator
 * @ClassName: PrestoEvaluator
 * @Description:
 * @author: tao
 * @date: Create in 2018-05-24 14:17
 **/
public class PrestoEvaluator {

    public static long execute(String jdbcUrl, Properties jdbcProperties, String tableName, String columns, String orderByColumn) {
        String sql = "";
        long start = 0L, end = 0L;
        try
        {
            Thread.sleep(1000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties)) {
            Statement statement = connection.createStatement();
            sql = "select " + columns + " from " + tableName + " order by " + orderByColumn + " limit 10";
             start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.next();
            end = System.currentTimeMillis();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("SQL: " + sql);
        }
        return end - start;
    }
}
