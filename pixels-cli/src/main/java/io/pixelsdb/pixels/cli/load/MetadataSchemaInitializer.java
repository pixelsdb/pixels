/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.cli.load;

import io.pixelsdb.pixels.common.metadata.MetadataDbType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates metadata tables in the configured metadata database.
 *
 * @author hank
 * @create 2026-09-02
 */
public class MetadataSchemaInitializer
{
    private static final Logger log = LogManager.getLogger(MetadataSchemaInitializer.class);

    private MetadataSchemaInitializer() { }

    /**
     * Detect the metadata database type from {@code pixels.properties} and execute
     * the matching CREATE TABLE script.
     *
     * @return the number of SQL statements that were executed successfully
     */
    public static int initialize() throws Exception
    {
        ConfigFactory config = ConfigFactory.Instance();
        String driver = config.getProperty("metadata.db.driver");
        String url = config.getProperty("metadata.db.url");
        String user = config.getProperty("metadata.db.user");
        String pass = config.getProperty("metadata.db.password");
        MetadataDbType dbType = MetadataDbType.from(driver, url);
        return initialize(dbType, driver, url, user, pass);
    }

    /**
     * Execute the CREATE TABLE script of the given metadata database type.
     *
     * @return the number of SQL statements that were executed successfully
     */
    public static int initialize(MetadataDbType dbType, String driver, String url,
                                 String user, String pass) throws Exception
    {
        if (driver == null || driver.isEmpty())
        {
            throw new IllegalArgumentException("metadata.db.driver is not set");
        }
        if (url == null || url.isEmpty())
        {
            throw new IllegalArgumentException("metadata.db.url is not set");
        }

        Class.forName(driver);
        List<String> statements = loadStatements(dbType.getSchemaResource());
        int executed = 0;
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement())
        {
            for (String sql : statements)
            {
                try
                {
                    stmt.execute(sql);
                    executed++;
                }
                catch (SQLException e)
                {
                    if (isAlreadyExists(e))
                    {
                        log.warn("skip existing metadata object: {}", summarize(sql));
                    }
                    else
                    {
                        throw new SQLException("failed to execute: " + summarize(sql), e);
                    }
                }
            }
        }
        return executed;
    }

    private static List<String> loadStatements(String resource) throws IOException
    {
        InputStream in = MetadataSchemaInitializer.class.getClassLoader().getResourceAsStream(resource);
        if (in == null)
        {
            throw new IOException("metadata schema resource not found: " + resource);
        }

        StringBuilder current = new StringBuilder();
        List<String> statements = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)))
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                String trimmed = stripLineComment(line).trim();
                if (trimmed.isEmpty())
                {
                    continue;
                }
                current.append(trimmed).append(' ');
                if (trimmed.endsWith(";"))
                {
                    String sql = current.toString().trim();
                    sql = sql.substring(0, sql.length() - 1).trim();
                    if (!sql.isEmpty())
                    {
                        statements.add(sql);
                    }
                    current.setLength(0);
                }
            }
        }
        String remaining = current.toString().trim();
        if (!remaining.isEmpty())
        {
            statements.add(remaining);
        }
        return statements;
    }

    private static String stripLineComment(String line)
    {
        int comment = line.indexOf("--");
        if (comment < 0)
        {
            return line;
        }
        return line.substring(0, comment);
    }

    private static boolean isAlreadyExists(SQLException e)
    {
        String state = e.getSQLState();
        if ("42S01".equals(state) || "42S11".equals(state) || "X0Y32".equals(state))
        {
            return true;
        }
        String message = e.getMessage();
        return message != null && message.toLowerCase().contains("already exists");
    }

    private static String summarize(String sql)
    {
        String compact = sql.replaceAll("\\s+", " ");
        return compact.length() > 120 ? compact.substring(0, 117) + "..." : compact;
    }
}
