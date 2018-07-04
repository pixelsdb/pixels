package cn.edu.ruc.iir.pixels.presto.evaluator;

import com.facebook.presto.spi.PrestoException;

import java.sql.*;
import java.util.Properties;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_SQL_EXECUTE_ERROR;
import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_THREAD_ERROR;

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
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new PrestoException(PIXELS_THREAD_ERROR, e);
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
            System.out.println("SQL: " + sql);
            System.out.println("Error msg: " + e.getMessage());
            throw new PrestoException(PIXELS_SQL_EXECUTE_ERROR, e);
        }
        return end - start;
    }

    public static long executeSQL(String jdbcUrl, Properties jdbcProperties, String sql, String id) {
        long start = 0L, end = 0L;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new PrestoException(PIXELS_THREAD_ERROR, e);
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties)) {
            Statement statement = connection.createStatement();
            start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSet.next();
            end = System.currentTimeMillis();
            statement.close();
        } catch (SQLException e) {
            System.out.println("SQL: " + id + "\n" + sql);
            System.out.println("Error msg: " + e.getMessage());
        }
        return end - start;
    }
}
