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

import io.pixelsdb.pixels.common.metadata.MetadataService;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory that creates a PixelsSchema.
 */
public class PixelsSchemaFactory implements SchemaFactory
{
    private static MetadataService metadataService;

    public PixelsSchemaFactory(MetadataService metadataService)
    {
        PixelsSchemaFactory.metadataService = metadataService;
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand)
    {
        return new PixelsSchema(name, metadataService);
    }
}
