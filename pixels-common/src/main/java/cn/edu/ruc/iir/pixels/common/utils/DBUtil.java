package cn.edu.ruc.iir.pixels.common.utils;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil
{
    private static DBUtil INSTANCE = new DBUtil();

    public static DBUtil Instance ()
    {
        return INSTANCE;
    }

    private Connection connection = null;
    private Logger log = LogFactory.Instance().getLog();

    private String url;
    private String user;
    private String pass;

    private DBUtil()
    {
        try {
            ConfigFactory config = ConfigFactory.Instance();
            String driver = config.getProperty("metadata.db.driver");
            url = config.getProperty("metadata.db.url");
            user = config.getProperty("metadata.db.user");
            pass = config.getProperty("metadata.db.password");

            Class.forName(driver);
            this.connection = DriverManager.getConnection(url, user, pass);
        } catch (Exception e) {
            log.error("Connection error: " + e.getMessage());
        }
    }

    public Connection getConnection ()
    {
        try
        {
            if (this.connection == null || this.connection.isValid(1000) == false)
            {
                this.connection = DriverManager.getConnection(url, user, pass);
            }
            return this.connection;
        } catch (SQLException e)
        {
            log.error("Connection error: " + e.getMessage());
            return null;
        }
    }

    public void close ()
    {
        try
        {
            if (this.connection != null)
            {
                this.connection.close();
            }
        } catch (SQLException e) {
            log.error("Close error: " + e.getMessage());
        }
    }
}
