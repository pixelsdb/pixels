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
package io.pixelsdb.pixels.daemon.metadata;

import io.pixelsdb.pixels.common.utils.ConfigFactory;

/**
 * Supported metadata database backends. The type is detected from
 * {@code metadata.db.driver} and {@code metadata.db.url} in pixels.properties.
 *
 * @author hank
 * @create 2026-09-02
 */
public enum MetadataDbType
{
    MYSQL("pixels_metadata_mysql.sql"),
    DERBY("pixels_metadata_derby.sql");

    private final String schemaResource;

    MetadataDbType(String schemaResource)
    {
        this.schemaResource = schemaResource;
    }

    /**
     * @return the classpath resource that contains CREATE TABLE statements for this backend
     */
    public String getSchemaResource()
    {
        return this.schemaResource;
    }

    /**
     * Detect the metadata database type from {@code pixels.properties}.
     */
    public static MetadataDbType fromConfig()
    {
        ConfigFactory config = ConfigFactory.Instance();
        return from(config.getProperty("metadata.db.driver"), config.getProperty("metadata.db.url"));
    }

    /**
     * Detect the metadata database type from the JDBC driver class and URL.
     */
    public static MetadataDbType from(String driver, String url)
    {
        String driverLower = driver == null ? "" : driver.toLowerCase();
        String urlLower = url == null ? "" : url.toLowerCase();
        if (driverLower.contains("derby") || urlLower.contains("jdbc:derby"))
        {
            return DERBY;
        }
        if (driverLower.contains("mysql") || urlLower.contains("jdbc:mysql"))
        {
            return MYSQL;
        }
        throw new IllegalArgumentException("unsupported metadata database, driver=" +
                driver + ", url=" + url);
    }
}
