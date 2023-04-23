/*
 * Copyright 2023 PixelsDB.
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @create 2023-03-28
 * @author hank
 */
public class QueryEngineConns
{
    private static final QueryEngineConns instance = new QueryEngineConns();

    public static QueryEngineConns Instance()
    {
        return instance;
    }

    private Map<String, QueryEngineConn> connections = new HashMap<>();

    private QueryEngineConns() {}

    public synchronized void openConn(String name, String driver, String url, Properties properties)
            throws SQLException
    {
        try
        {
            Class.forName(driver);
            Connection connection = DriverManager.getConnection(url, properties);
            this.connections.put(name, new QueryEngineConn(connection, driver, url, properties));
        } catch (ClassNotFoundException | SQLException e)
        {
            throw new SQLException("failed to open connection", e);
        }
    }

    public synchronized Connection getConnection(String name) throws SQLException
    {
        QueryEngineConn conn = this.connections.get(name);
        if (conn == null)
        {
            throw new SQLException("connection does not exist");
        }
        if (!conn.sqlConn.isValid(30))
        {
            if (!conn.sqlConn.isClosed())
            {
                conn.sqlConn.close();
            }
            conn.sqlConn = DriverManager.getConnection(conn.url, conn.properties);
        }
        return conn.sqlConn;
    }

    public synchronized void closeConn(String name) throws SQLException
    {
        QueryEngineConn conn = this.connections.get(name);
        if (conn == null)
        {
            throw new SQLException("connection does not exist");
        }
        if (!conn.sqlConn.isClosed())
        {
            conn.sqlConn.close();
        }
        this.connections.remove(name);
    }

    private static class QueryEngineConn
    {
        Connection sqlConn;
        String driver;
        String url;
        Properties properties;

        public QueryEngineConn(Connection sqlConn, String driver, String url, Properties properties)
        {
            this.sqlConn = sqlConn;
            this.driver = driver;
            this.url = url;
            this.properties = properties;
        }
    }
}
