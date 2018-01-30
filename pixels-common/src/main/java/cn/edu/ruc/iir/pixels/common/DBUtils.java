package cn.edu.ruc.iir.pixels.common;

import io.airlift.log.Logger;

import java.sql.*;

public class DBUtils {

    private static Logger log = Logger.get(DBUtils.class);
    private static DBUtils instance = null;

    private static String DRIVER;
    private static String URL;
    private static String USERID;
    private static String USERPASSWORD;
    private static Connection conn = null;
    private PreparedStatement psmt = null;
    private ResultSet rs = null;
    private Statement statement = null;

    private DBUtils() {
        try {
            ConfigFactory config = ConfigFactory.Instance();
            DRIVER = config.getProperty("driver");
            URL = config.getProperty("url");
            USERID = config.getProperty("user");
            USERPASSWORD = config.getProperty("password");

            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URL, USERID, USERPASSWORD);
        } catch (Exception e) {
            log.error("Connection error! errmsg: " + e.getMessage());
        }
    }

    public static DBUtils Instance() {
        if (instance == null) {
            instance = new DBUtils();
        }
        return instance;
    }

    public void close() {
        try {
            if (rs != null)
                rs.close();
            if (statement != null)
                statement.close();
            if (psmt != null)
                psmt.close();
            if (conn != null)
                conn.close();
        } catch (Exception e) {
            log.error("Close error! errmsg: " + e.getMessage());
        }
    }

    public ResultSet getQuery(String SQL) {
        try {
            statement = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            rs = statement.executeQuery(SQL);
        } catch (Exception e) {
            log.error("Select from sql server error! errmsg: " + e.getMessage());
        }
        return rs;
    }

    public ResultSet getQuery(String sql, String[] arr) {
        try {
            psmt = conn.prepareStatement(sql); // select * from ta where ta.a =? and ta.b =?
            if (arr != null && arr.length > 0) {
                for (int i = 0; i < arr.length; i++) {
                    psmt.setString(i + 1, arr[i]);
                }
            }
            rs = psmt.executeQuery();
        } catch (SQLException e) {
            log.error("Select from sql server error! errmsg: " + e.getMessage());
        }
        return rs;
    }

    public boolean getUpdate(String SQL) {
        boolean flag = false;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            flag = statement.execute(SQL);
        } catch (Exception e) {
            log.error("Execute sql error! errmsg: " + e.getMessage());
        }
        return flag;
    }

    public int getUpdate(String sql, String[] arr) {
        int result = 0;
        try {
            psmt = conn.prepareStatement(sql);
            if (arr != null && arr.length > 0) {
                for (int i = 0; i < arr.length; i++) {
                    psmt.setString(i + 1, arr[i]);
                }
            }
            result = psmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Execute sql error! errmsg: " + e.getMessage());
        }
        return result;
    }
}
