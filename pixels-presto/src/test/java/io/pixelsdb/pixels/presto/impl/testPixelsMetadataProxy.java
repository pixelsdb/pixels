/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.presto.impl;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @author: tao
 * @date: Create in 2018-01-27 11:15
 **/
public class testPixelsMetadataProxy
{
    private PixelsMetadataProxy pixelsMetadataProxy = null;
    private final Logger log = Logger.getLogger(testPixelsMetadataProxy.class.getName());

    @Before
    public void init ()
    {
        PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
        this.pixelsMetadataProxy = new PixelsMetadataProxy(config, null);
    }

    @Test
    public void testGetSchemaNames() throws MetadataException
    {
        List<String> schemaList = pixelsMetadataProxy.getSchemaNames();
        System.out.println(schemaList.toString());
        log.info("Size: " + schemaList.size());
    }

    @Test
    public void testGetTableNames() throws MetadataException
    {
        List<String> tablelist = pixelsMetadataProxy.getTableNames("pixels");
        System.out.println(tablelist.toString());
    }

    @Test
    public void getTable() throws MetadataException
    {
        Table table = pixelsMetadataProxy.getTable("default", "test");
        System.out.println(table.getName());
        System.out.println(table.getType());
        System.out.println(table.getStorageScheme());
    }
}
