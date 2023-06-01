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
package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema mapped onto Pixels internal schema representation.
 */
public class PixelsSchema extends AbstractSchema
{
    private final String name;
    private final MetadataService metadataService;

    PixelsSchema(String name, MetadataService metadataService)
    {
        this.name = name;
        this.metadataService = metadataService;
    }

    @Override
    protected Map<String, Table> getTableMap()
    {
        final Map<String, Table> tables = new HashMap<>();
        try
        {
            List<io.pixelsdb.pixels.common.metadata.domain.Table> pixelsTables = this.metadataService.getTables(this.name);
            for (io.pixelsdb.pixels.common.metadata.domain.Table t : pixelsTables)
            {
                String tableName = t.getName();
                tables.put(tableName, new PixelsTable(this.name, tableName, this.metadataService));
            }
            return tables;
        } catch (MetadataException e)
        {
            throw new RuntimeException(e);
        }
    }
}
