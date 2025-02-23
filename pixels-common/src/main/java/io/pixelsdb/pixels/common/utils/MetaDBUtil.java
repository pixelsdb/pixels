/*
 * Copyright 2017 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.common.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author hank
 */
public class MetaDBUtil
{
    private static final Logger log = LogManager.getLogger(MetaDBUtil.class);

    private static final MetaDBUtil INSTANCE = new MetaDBUtil();

    public static MetaDBUtil Instance()
    {
        return INSTANCE;
    }

    private Connection connection = null;

    private String url;
    private String user;
    private String pass;

    private MetaDBUtil()
    {
        try
        {
            ConfigFactory config = ConfigFactory.Instance();
            String driver = config.getProperty("metadata.db.driver");
            url = config.getProperty("metadata.db.url");
            user = config.getProperty("metadata.db.user");
            pass = config.getProperty("metadata.db.password");

            Class.forName(driver);
            this.connection = DriverManager.getConnection(url, user, pass);
        }
        catch (Exception e)
        {
            log.error("Connection error: ", e);
        }
    }

    public Connection getConnection()
    {
        try
        {
            if (this.connection == null || !this.connection.isValid(30))
            {
                this.connection = DriverManager.getConnection(url, user, pass);
            }
            return this.connection;
        }
        catch (SQLException e)
        {
            log.error("Connection error: " + e.getMessage());
            return null;
        }
    }

    public void close()
    {
        try
        {
            if (this.connection != null)
            {
                this.connection.close();
            }
        }
        catch (SQLException e)
        {
            log.error("Close error: " + e.getMessage());
        }
    }
}
