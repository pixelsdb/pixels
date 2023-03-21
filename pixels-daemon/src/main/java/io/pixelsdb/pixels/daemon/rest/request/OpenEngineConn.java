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
package io.pixelsdb.pixels.daemon.rest.request;

import java.util.Properties;

/**
 * The request to open a query engine using jdbc url.
 * Created at: 3/17/23
 * Author: hank
 */
public class OpenEngineConn
{
    private String connName;
    private Properties properties;
    /**
     * JDBC driver.
     */
    private String driver;
    /**
     * JDBC url.
     */
    private String url;

    public OpenEngineConn(String connName, Properties properties, String driver, String url)
    {
        this.connName = connName;
        this.properties = properties;
        this.driver = driver;
        this.url = url;
    }

    public String getConnName()
    {
        return connName;
    }

    public void setConnName(String connName)
    {
        this.connName = connName;
    }

    public Properties getProperties()
    {
        return properties;
    }

    public void setProperties(Properties properties)
    {
        this.properties = properties;
    }

    public String getDriver()
    {
        return driver;
    }

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }
}
